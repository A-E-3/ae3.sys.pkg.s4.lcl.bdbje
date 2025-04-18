package ru.myx.ae3.vfs.s4.lcl.bdbj;

import java.util.function.Function;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.ForwardCursor;
import com.sleepycat.je.Get;

import ru.myx.ae3.Engine;
import ru.myx.ae3.console.Console;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.know.Guid;
import ru.myx.ae3.vfs.TreeLinkType;
import ru.myx.sapi.FormatSAPI;

final class CheckScanTreeRecords //
		implements
			Function<CheckContext, Boolean> {
	
	public static final boolean check(final CheckContext context) {
		
		final RowTree recordTree = new RowTree();
		final RowItem recordTreeSourceItem = new RowItem();
		final RowTreeUsage recordTreeUsage = new RowTreeUsage();
		final RowTreeIndex recordTreeIndex = new RowTreeIndex();
		
		final Console console = Exec.currentProcess().getConsole();
		
		final DatabaseEntry key = context.key;
		final DatabaseEntry value = context.value;
		
		/** try ( **/
		final ForwardCursor forwardCursor = context.forwardCursor(TableDefinition.TREE);
		/** ) **/
		{
			long lastCheckedItemLuid = -1L;
			int left = CheckContext.BATCH_LIMIT;
			for (;;) {
				if (null == forwardCursor.get(key, value, Get.NEXT, WorkerBdbj.RO_FOR_SCAN)) {
					return false;
				}
				mainTableCheck : {
					++context.checks;
					try {
						RowTree.materializeRowTree(recordTree, key.getData(), value.getData());
					} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
						final String op = "invalid tree record, luid" + recordTree.key0_luid6 + ", e: " + FormatSAPI.plainTextDescribe(e);
						++context.errors;
						/** main check table: purging of corrupted records is the default
						 * behavoir **/
						if (context.isFix && (context.isPurge || !context.isRecover)) {
							context.internPurgeRecord(context.local.dbTree, key, op);
							break mainTableCheck;
						}
						console.sendMessage(op);
						break mainTableCheck;
					}
					if (context.isVerbose) {
						console.sendMessage("checking: " + FormatSAPI.jsObject(recordTree));
					}
					if (!recordTree.key1_keyX.isValid()) {
						console.sendMessage("invalid key guid: " + FormatSAPI.jsObject(recordTree));
						++context.errors;
						break mainTableCheck;
					}
					if (!recordTree.val2_targetX.isValid()) {
						console.sendMessage("invalid target guid: " + FormatSAPI.jsObject(recordTree));
						++context.errors;
						break mainTableCheck;
					}
					
					rowItemCheck : if (lastCheckedItemLuid == recordTree.key0_luid6) {
						// skip RowItemCheck
					} else {
						lastCheckedItemLuid = recordTree.key0_luid6;
						boolean found = false;
						try {
							found = RowItem.findRowItemByKey(//
									recordTreeSourceItem,
									context.cursorItem(),
									key,
									value,
									recordTree.key0_luid6//
							);
						} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
							console.sendMessage("invalid item record for tree, tree: " + FormatSAPI.jsObject(recordTree) + ", e: " + FormatSAPI.plainTextDescribe(e));
							found = false;
						}
						if (!found) {
							final Cursor cursorTree = context.cursorTree();
							final String op = "orphaned tree record, tree: " + FormatSAPI.jsObject(recordTree);
							final boolean persistent = TreeLinkType.valueForIndex(recordTree.val0_mode1).blocksGarbageCollection();
							if (context.isFix && persistent && context.isRecover) {
								console.sendMessage("recover, not yet: " + op);
								++context.errors;
								break rowItemCheck;
							}
							if (context.isFix && (!persistent || context.isPurge)) {
								RowTree.setupTreeKey(//
										key,
										context.byteBuffer,
										recordTree.key0_luid6,
										recordTree.key1_keyX//
								);
								if (null == cursorTree.get(key, null, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN)) {
									console.sendMessage("error purging (not found) " + op);
									++context.errors;
									break rowItemCheck;
								}
								if (null == cursorTree.delete(WorkerBdbj.WO_FOR_DROP)) {
									console.sendMessage("error purging (already deleted) " + op);
									++context.errors;
									break rowItemCheck;
								}
								console.sendMessage("purged " + op);
								++context.fixes;
								break rowItemCheck;
							}
							console.sendMessage(op);
							++context.errors;
							break rowItemCheck;
						}
					}
					
					rowTreeUsageCheck : if (!recordTree.val2_targetX.isInline()) {
						final boolean persistent = TreeLinkType.valueForIndex(recordTree.val0_mode1).blocksGarbageCollection();
						final Cursor cursorTreeUsage = context.cursorTreeUsage();
						
						boolean found = false;
						try {
							found = RowTreeUsage.findRowTreeUsageByKey(//
									recordTreeUsage,
									cursorTreeUsage,
									key,
									recordTree.val2_targetX,
									recordTree.key0_luid6,
									recordTree.key1_keyX//
							);
						} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
							console.sendMessage("invalid treeUsage record, tree: " + FormatSAPI.jsObject(recordTree) + ", e: " + FormatSAPI.plainTextDescribe(e));
							found = false;
						}
						
						/** no matching record **/
						if (!found) {
							/** no matching record, exactly as expected **/
							if (!persistent) {
								break rowTreeUsageCheck;
							}
							
							final String op = "no treeUsage for tree: " + FormatSAPI.jsObject(recordTree);
							if (context.isFix && (!context.isPurge || context.isRecover)) {
								RowTreeUsage.serializeRowTreeUsage(
										context.cursorTreeUsage(),
										context.byteBuffer,
										key,
										value,
										recordTree.val2_targetX,
										recordTree.key0_luid6,
										recordTree.key1_keyX);
								console.sendMessage("fixed " + op);
								++context.fixes;
								break rowTreeUsageCheck;
							}
							console.sendMessage(op);
							++context.errors;
							break rowTreeUsageCheck;
						}
						
						/** found, exactly as expected **/
						if (persistent) {
							break rowTreeUsageCheck;
						}
						
						final String op = "extra treeUsage for tree: " + FormatSAPI.jsObject(recordTree);
						if (context.isFix && context.isPurge) {
							RowTreeUsage.setupTreeUsageKey(//
									key,
									context.byteBuffer,
									recordTree.val2_targetX,
									recordTree.key0_luid6,
									recordTree.key1_keyX//
							);
							if (null == cursorTreeUsage.get(key, null, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN)) {
								console.sendMessage("error purging (not found) " + op);
								++context.errors;
								break rowTreeUsageCheck;
							}
							if (null == cursorTreeUsage.delete(WorkerBdbj.WO_FOR_DROP)) {
								console.sendMessage("error purging (already deleted) " + op);
								++context.errors;
								break rowTreeUsageCheck;
							}
							console.sendMessage("purged " + op);
							++context.fixes;
							break rowTreeUsageCheck;
						}
						console.sendMessage(op);
						++context.errors;
						break rowTreeUsageCheck;
					}
					
					rowTreeIndexCheck : {
						final boolean searchable = TreeLinkType.valueForIndex(recordTree.val0_mode1).allowsSearchIndexing();
						final Cursor cursorTreeIndex = context.cursorTreeIndex();
						
						boolean found = false;
						try {
							found = RowTreeIndex.findRowTreeIndexByKey(//
									recordTreeIndex,
									cursorTreeIndex,
									key,
									recordTree.key1_keyX,
									recordTree.val2_targetX,
									recordTree.key0_luid6//
							);
						} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
							console.sendMessage("invalid treeIndex record, tree: " + FormatSAPI.jsObject(recordTree) + ", e: " + FormatSAPI.plainTextDescribe(e));
							found = false;
						}
						final String op = "treeIndex for tree: " + FormatSAPI.jsObject(recordTree);
						if (!found) {
							if (!searchable) {
								break rowTreeIndexCheck;
							}
							if (context.isFix && (!context.isPurge || context.isRecover)) {
								RowTreeIndex.serializeRowTreeIndex(
										context.cursorTreeIndex(),
										context.byteBuffer,
										key,
										value,
										recordTree.key1_keyX,
										recordTree.val2_targetX,
										recordTree.key0_luid6);
								console.sendMessage("fixed " + op);
								++context.fixes;
								break rowTreeIndexCheck;
							}
							console.sendMessage("no " + op);
							++context.errors;
							break rowTreeIndexCheck;
						}
						/* found */ {
							if (searchable) {
								break rowTreeIndexCheck;
							}
							if (context.isFix && context.isPurge) {
								RowTreeIndex.setupTreeIndexKey(//
										key,
										context.byteBuffer,
										recordTree.key1_keyX,
										recordTree.val2_targetX,
										recordTree.key0_luid6//
								);
								if (null == cursorTreeIndex.get(key, null, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN)) {
									console.sendMessage("error purging (not found) " + op);
									++context.errors;
									break rowTreeIndexCheck;
								}
								if (null == cursorTreeIndex.delete(WorkerBdbj.WO_FOR_DROP)) {
									console.sendMessage("error purging (already deleted) " + op);
									++context.errors;
									break rowTreeIndexCheck;
								}
								console.sendMessage("purged " + op);
								++context.fixes;
								break rowTreeIndexCheck;
							}
							console.sendMessage("extra " + op);
							++context.errors;
							break rowTreeIndexCheck;
						}
					}
					break mainTableCheck;
				}
				if (--context.left == 0) {
					// check size
					console.sendMessage("check size limit reached.");
					return false;
				}
				if (context.stop < Engine.fastTime()) {
					// check duration
					console.sendMessage("check time limit reached.");
					return false;
				}
				if (--left == 0) {
					// batch limit
					context.nextBatch();
					return true;
				}
			}
		}
	}
	
	@SuppressWarnings("unused")
	private static long findMakeRecoveryFolder(final CheckContext context) {
		
		final RowItemGuid rootItemGuid = new RowItemGuid();
		if (!RowItemGuid.findRowItemGuidByGuid(//
				rootItemGuid,
				context.cursorItemGuid(),
				context.byteBuffer,
				context.key,
				context.value,
				Guid.GUID_NULL//
		)) {
			throw new IllegalStateException("No root record!");
		}
		final RowTree recoveryReference = new RowTree();
		final RowItemGuid recoveryItemGuid = new RowItemGuid();
		if (RowTree.findRowTreeByKey(//
				recoveryReference,
				context.cursorTree(),
				context.key,
				context.value,
				rootItemGuid.key1_luid6,
				Guid.forString(".recovered")//
		)) {
			if (!RowItemGuid.findRowItemGuidByGuid(//
					recoveryItemGuid,
					context.cursorItemGuid(),
					context.byteBuffer,
					context.key,
					context.value,
					recoveryReference.val2_targetX//
			)) {
				throw new IllegalStateException("No 'recovery' record, that supposed to exist!");
			}
			return recoveryItemGuid.key1_luid6;
		}
		{
			final Guid guid = Guid.createGuid384();
			recoveryItemGuid.key0_azimuth4 = guid.hashCode();
			recoveryItemGuid.key1_luid6 = context.local.nextSequenceLuid();
			recoveryItemGuid.val0_schedule2 = 0;
			recoveryItemGuid.val1_guidX = guid;
		}
		throw new IllegalStateException("Not yet!");
	}
	
	@Override
	public Boolean apply(final CheckContext context) {
		
		return CheckScanTreeRecords.check(context)
			? Boolean.TRUE
			: Boolean.FALSE;
	}
	
}
