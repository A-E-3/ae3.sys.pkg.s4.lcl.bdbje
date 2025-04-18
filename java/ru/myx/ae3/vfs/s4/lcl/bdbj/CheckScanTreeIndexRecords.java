package ru.myx.ae3.vfs.s4.lcl.bdbj;

import java.util.function.Function;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.ForwardCursor;
import com.sleepycat.je.Get;

import ru.myx.ae3.Engine;
import ru.myx.ae3.console.Console;
import ru.myx.ae3.exec.Exec;
import ru.myx.sapi.FormatSAPI;

final class CheckScanTreeIndexRecords //
		implements
			Function<CheckContext, Boolean> {

	public static final boolean check(final CheckContext context) {

		final RowTree recordIndexTree = new RowTree();
		final RowTreeIndex recordIndex = new RowTreeIndex();

		final Console console = Exec.currentProcess().getConsole();

		final DatabaseEntry key = context.key;
		final DatabaseEntry value = context.value;

		/** try ( **/
		final ForwardCursor forwardCursor = context.forwardCursor(TableDefinition.TREE_INDEX);
		/** ) **/
		{
			int left = CheckContext.BATCH_LIMIT;
			for (;;) {
				if (null == forwardCursor.get(key, value, Get.NEXT, WorkerBdbj.RO_FOR_SCAN)) {
					return false;
				}
				mainTableCheck : {
					++context.checks;
					try {
						RowTreeIndex.materializeRowTreeIndex(recordIndex, key.getData());
					} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
						final String op = //
								"invalid treeIndex record, luid: " + recordIndex.key2_luid6 + //
										", length: " + key.getData().length + //
										", index: " + e.getMessage() + //
										", e: " + FormatSAPI.plainTextDescribe(e)//
						;
						++context.errors;
						/** main check table: purging of corrupted records is the default
						 * behavoir **/
						if (context.isFix && (context.isPurge || !context.isRecover)) {
							context.internPurgeRecord(context.local.dbTreeIndex, key, op);
							break mainTableCheck;
						}
						console.sendMessage(op);
						break mainTableCheck;
					}
					if (context.isVerbose) {
						console.sendMessage("checking: " + FormatSAPI.jsObject(recordIndex));
					}
					rowTreeCheck : {
						boolean found = false;
						try {
							found = RowTree.findRowTreeByKey(//
									recordIndexTree,
									context.cursorTree(),
									key,
									value,
									recordIndex.key2_luid6,
									recordIndex.key0_keyX//
							);
						} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
							console.sendMessage("invalid tree record, treeIndex key: " + FormatSAPI.jsObject(recordIndex) + ", e: " + FormatSAPI.plainTextDescribe(e));
							found = false;
						}
						if (found) {
							break rowTreeCheck;
						}
						final Cursor cursorTreeIndex = context.cursorTreeIndex();
						final String op = "orphaned treeIndex record, treeIndex key: " + FormatSAPI.jsObject(recordIndex);
						if (context.isFix && context.isPurge) {
							RowTreeIndex.setupTreeIndexKey(//
									key,
									context.byteBuffer,
									recordIndex.key0_keyX,
									recordIndex.key1_targetX,
									recordIndex.key2_luid6//
							);
							if (null == cursorTreeIndex.get(key, null, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN)) {
								console.sendMessage("error purging (not found) " + op);
								++context.errors;
								break rowTreeCheck;
							}
							if (null == cursorTreeIndex.delete(WorkerBdbj.WO_FOR_DROP)) {
								console.sendMessage("error purging (already deleted) " + op);
								++context.errors;
								break rowTreeCheck;
							}
							console.sendMessage("purged " + op);
							++context.fixes;
							break rowTreeCheck;
						}
						console.sendMessage(op);
						++context.errors;
						break rowTreeCheck;
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

	@Override
	public Boolean apply(final CheckContext context) {

		return CheckScanTreeIndexRecords.check(context)
			? Boolean.TRUE
			: Boolean.FALSE;
	}

}
