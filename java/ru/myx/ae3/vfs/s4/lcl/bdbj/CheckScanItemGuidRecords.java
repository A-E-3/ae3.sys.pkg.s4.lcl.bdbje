package ru.myx.ae3.vfs.s4.lcl.bdbj;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.ForwardCursor;
import com.sleepycat.je.Get;

import ru.myx.ae3.Engine;
import ru.myx.ae3.console.Console;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.know.Guid;
import ru.myx.ae3.vfs.s4.driver.S4Driver;
import ru.myx.sapi.FormatSAPI;

final class CheckScanItemGuidRecords //
		implements
			Function<CheckContext, Boolean> {
	
	public static final boolean check(final CheckContext context) {
		
		final RowItemGuid recordItemGuid = new RowItemGuid();
		final RowItem recordItem = new RowItem();
		final RowItemQueue recordItemQueue = new RowItemQueue();
		final RowTail recordTail = new RowTail();
		
		final Console console = Exec.currentProcess().getConsole();
		
		final DatabaseEntry key = context.key;
		final DatabaseEntry value = context.value;
		
		/** try ( **/
		final ForwardCursor forwardCursor = context.forwardCursor(TableDefinition.ITEM_GUID);
		/** ) **/
		{
			final Map<Guid, Boolean> seenGuids = new TreeMap<>();
			int left = CheckContext.BATCH_LIMIT;
			int previousAzimuth = 0;
			for (;;) {
				if (null == forwardCursor.get(key, value, Get.NEXT, WorkerBdbj.RO_FOR_SCAN)) {
					return false;
				}
				mainTableCheck : {
					++context.checks;
					try {
						RowItemGuid.materializeRowItemGuid(recordItemGuid, key.getData(), value.getData());
					} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
						final String op = "invalid itemGuid record, luid" + recordItemGuid.key1_luid6 + ", e: " + FormatSAPI.plainTextDescribe(e);
						++context.errors;
						/** main check table: purging of corrupted records is the default
						 * behavoir **/
						if (context.isFix && (context.isPurge || !context.isRecover)) {
							context.internPurgeRecord(context.local.dbItemGuid, key, op);
							break mainTableCheck;
						}
						console.sendMessage(op);
						break mainTableCheck;
					}
					if (context.isVerbose) {
						console.sendMessage("checking: " + FormatSAPI.jsObject(recordItemGuid));
					}
					if (recordItemGuid.key0_azimuth4 != recordItemGuid.val1_guidX.hashCode()) {
						final String op = "invalid azimuth: expect=" + recordItemGuid.val1_guidX.hashCode() + ", record=" + FormatSAPI.jsObject(recordItemGuid);
						++context.errors;
						if (context.isFix && context.isPurge) {
							context.internPurgeRecord(context.local.dbItemGuid, key, op);
							break mainTableCheck;
						}
						console.sendMessage(op);
						break mainTableCheck;
					}
					if (!recordItemGuid.val1_guidX.isValid()) {
						final String op = "invalid guid: " + FormatSAPI.jsObject(recordItemGuid);
						++context.errors;
						if (context.isFix && context.isPurge) {
							context.internPurgeRecord(context.local.dbItemGuid, key, op);
							break mainTableCheck;
						}
						console.sendMessage(op);
						break mainTableCheck;
					}
					if (recordItemGuid.val1_guidX.isPrimitive() && recordItemGuid.val1_guidX != Guid.GUID_NULL) {
						final String op = "primitive guid: " + FormatSAPI.jsObject(recordItemGuid);
						++context.errors;
						if (context.isFix && context.isPurge) {
							context.internPurgeRecord(context.local.dbItemGuid, key, op);
							break mainTableCheck;
						}
						console.sendMessage(op);
						break mainTableCheck;
					}
					if (recordItemGuid.val1_guidX.isInline() && recordItemGuid.val1_guidX != Guid.GUID_NULL) {
						final String op = "inline guid: " + FormatSAPI.jsObject(recordItemGuid);
						++context.errors;
						if (context.isFix && context.isPurge) {
							context.internPurgeRecord(context.local.dbItemGuid, key, op);
							break mainTableCheck;
						}
						console.sendMessage(op);
						break mainTableCheck;
					}
					if (recordItemGuid.val1_guidX.isBinary() && recordItemGuid.val1_guidX.getBinaryLength() > S4Driver.TAIL_CAPACITY) {
						final String op = "fat binary guid: " + FormatSAPI.jsObject(recordItemGuid);
						++context.errors;
						if (context.isFix && context.isPurge) {
							context.internPurgeRecord(context.local.dbItemGuid, key, op);
							break mainTableCheck;
						}
						console.sendMessage(op);
						break mainTableCheck;
					}
					
					boolean purgeRecord = false;
					purgeDuplicateRecord : if (seenGuids.get(recordItemGuid.val1_guidX) == Boolean.TRUE) {
						console.sendMessage("duplicate guid: " + FormatSAPI.jsObject(recordItemGuid));
						++context.errors;
						
						/** check existing tree records, but only for legit collections **/
						if (RowTree.guidIsTreeCollection(recordItemGuid.val1_guidX)) {
							/** check usage in tree (have children?) **/
							WorkerBdbj.writeLongAsLow6(context.byteBuffer, 0, recordItemGuid.key1_luid6);
							key.setData(context.byteBuffer, 0, 6);
							if (null != context.cursorTree().get(key, null, Get.SEARCH_GTE, null)) {
								if (recordItemGuid.key1_luid6 == WorkerBdbj.readLow6AsLong(key.getData(), 0)) {
									console.sendMessage("duplicate guid has children in tree, won't fix: " + recordItemGuid.key1_luid6);
									break purgeDuplicateRecord;
								}
							}
						}
						/** duplicate main table record found **/
						purgeRecord = true;
					}
					
					/** remember for duplicate checks **/
					{
						if (previousAzimuth != recordItemGuid.key0_azimuth4) {
							previousAzimuth = recordItemGuid.key0_azimuth4;
							seenGuids.clear();
						}
						seenGuids.put(recordItemGuid.val1_guidX, Boolean.TRUE);
					}
					
					runSecondaryChecks : for (;;) {
						rowItemCheck : {
							boolean found = false;
							try {
								found = RowItem.findRowItemByKey(//
										recordItem,
										context.cursorItem(),
										key,
										value,
										recordItemGuid.key1_luid6//
								);
							} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
								final String op = "invalid itemGuid's item record, guid: " + FormatSAPI.jsObject(recordItemGuid) + ", e: " + FormatSAPI.plainTextDescribe(e);
								++context.errors;
								/** secondary check table: purging of corrupted records must be
								 * explict **/
								if (context.isFix && context.isPurge) {
									context.internPurgeRecord(context.local.dbItem, key, op);
									break rowItemCheck;
								}
								console.sendMessage(op);
								found = false;
							}
							if (purgeRecord && context.isFix && context.isPurge) {
								if (!found) {
									break rowItemCheck;
								}
								if (found) {
									if (recordItemGuid.val1_guidX.equals(recordItem.val1_guidX)) {
										final String op = " purgable itemGuid's item record, luid: " + recordItemGuid.key1_luid6 + ", item: " + FormatSAPI.jsObject(recordItem);
										context.internPurgeRecord(context.local.dbItem, key, op);
										break rowItemCheck;
									}
									/** fall-through **/
								}
								/** fall-through **/
							}
							if (!found) {
								/** recover item record only in recover mode, it is more likely that
								 * itemGuid record is an orphan **/
								if (context.isFix && context.isRecover) {
									if (purgeRecord) {
										console.sendMessage("no item for purgable itemGuid, itemGuid: " + FormatSAPI.jsObject(recordItemGuid));
										++context.warnings;
										break rowItemCheck;
									}
									RowItem.serializeRowItem(//
											context.cursorItem(),
											context.byteBuffer,
											key,
											value,
											recordItemGuid.key1_luid6,
											recordItemGuid.val0_schedule2,
											recordItemGuid.val1_guidX//
									);
									console.sendMessage("fixed item for itemGuid, itemGuid: " + FormatSAPI.jsObject(recordItemGuid));
									++context.fixes;
									break rowItemCheck;
								}
								console.sendMessage("no item for itemGuid, itemGuid: " + FormatSAPI.jsObject(recordItemGuid));
								++context.errors;
								break rowItemCheck;
							}
							{
								/** actual item value is not matching our main record **/
								if (!recordItemGuid.val1_guidX.equals(recordItem.val1_guidX)) {
									final RowItemGuid recordGuidOther = new RowItemGuid();
									if (RowItemGuid.findRowItemGuidByKey(//
											recordGuidOther,
											context.cursorItemGuid(),
											key,
											value,
											recordItem.val1_guidX.hashCode(),
											recordItem.key0_luid6 //
									) && recordItem.val1_guidX.equals(recordGuidOther.val1_guidX)) {
										final String op = "phantom itemGuid record, itemGuid: " + FormatSAPI.jsObject(recordItemGuid) + ", guid:"
												+ FormatSAPI.jsObject(recordGuidOther.val1_guidX);
										++context.errors;
										if (context.isFix && context.isPurge) {
											WorkerBdbj.writeInt(context.byteBuffer, 0, recordItemGuid.key0_azimuth4);
											WorkerBdbj.writeLongAsLow6(context.byteBuffer, 4, recordItemGuid.key1_luid6);
											key.setData(context.byteBuffer, 0, 4 + 6);
											context.internPurgeRecord(context.local.dbItemGuid, key, op);
											break mainTableCheck;
										}
										console.sendMessage(op);
										break mainTableCheck;
									}
									{
										if (context.isFix && (!context.isPurge || context.isRecover)) {
											RowItem.serializeRowItem(//
													context.cursorItem(),
													context.byteBuffer,
													key,
													value,
													recordItemGuid.key1_luid6,
													recordItemGuid.val0_schedule2,
													recordItemGuid.val1_guidX//
											);
											console.sendMessage(
													"fixed Guid mismatch in item for itemGuid, itemGuid: " + FormatSAPI.jsObject(recordItemGuid) + ", item: "
															+ FormatSAPI.jsObject(recordItem));
											++context.fixes;
										} else {
											console.sendMessage("Guid mismatch in item for itemGuid, itemGuid: " + FormatSAPI.jsObject(recordItemGuid));
											++context.errors;
											break mainTableCheck;
										}
									}
								}
							}
							break rowItemCheck;
						}
						rowItemQueueCheck : {
							boolean found = false;
							try {
								found = RowItemQueue.findRowItemQueueByKey(//
										recordItemQueue,
										context.cursorItemQueue(),
										key,
										recordItemGuid.val0_schedule2,
										recordItemGuid.key1_luid6//
								);
							} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
								final String op = "invalid itemQueue record, guid: " + FormatSAPI.jsObject(recordItemGuid) + ", e: " + FormatSAPI.plainTextDescribe(e);
								++context.errors;
								/** secondary check table: purging of corrupted records must be
								 * explict **/
								if (context.isFix && context.isPurge) {
									context.internPurgeRecord(context.local.dbItemQueue, key, op);
									break rowItemQueueCheck;
								}
								console.sendMessage(op);
								found = false;
							}
							if (purgeRecord && context.isFix && context.isPurge) {
								if (found) {
									final String op = " purgable itemGuid's itemQueue record, luid: " + recordItemGuid.key1_luid6 + ", item: "
											+ FormatSAPI.jsObject(recordItemQueue);
									context.internPurgeRecord(context.local.dbItemQueue, key, op);
								}
								break rowItemQueueCheck;
							}
							if (!found) {
								/** fix queue (schedule) by default unless purge **/
								if (context.isFix && !context.isPurge) {
									RowItemQueue.serializeRowItemQueue(//
											context.cursorItemQueue(),
											context.byteBuffer,
											key,
											value,
											recordItemGuid.val0_schedule2,
											recordItemGuid.key1_luid6//
									);
									console.sendMessage("fixed itemQueue for itemGuid, itemGuid: " + FormatSAPI.jsObject(recordItemGuid));
									++context.fixes;
									break rowItemQueueCheck;
								}
								console.sendMessage("no itemQueue for itemGuid, itemGuid: " + FormatSAPI.jsObject(recordItemGuid));
								++context.errors;
								break rowItemQueueCheck;
							}
							break rowItemQueueCheck;
						}
						rowTailCheck : if (RowTail.guidIsBinaryTail(recordItemGuid.val1_guidX)) {
							boolean found = false;
							try {
								RowTail.setupTailKey(key, context.byteBuffer, recordItemGuid.key1_luid6);
								found = RowTail.findRowTailByKey(//
										recordTail,
										context.cursorTail(),
										key,
										value,
										recordItemGuid.key1_luid6//
								);
							} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
								final String op = "invalid itemGuid's tail record, guid: " + FormatSAPI.jsObject(recordItemGuid) + ", e: " + FormatSAPI.plainTextDescribe(e);
								++context.errors;
								/** secondary check table: purging of corrupted records must be
								 * explict **/
								if (context.isFix && context.isPurge) {
									context.internPurgeRecord(context.local.dbTail, key, op);
									break rowTailCheck;
								}
								console.sendMessage(op);
								found = false;
							}
							if (purgeRecord && context.isFix && context.isPurge) {
								if (found) {
									final String op = " purgable itemGuid's tail record, luid: " + recordItemGuid.key1_luid6 + ", item: " + FormatSAPI.jsObject(recordTail);
									RowTail.setupTailKey(key, context.byteBuffer, recordTail.key0_luid6);
									context.internPurgeRecord(context.local.dbTail, key, op);
									break rowTailCheck;
								}
								break rowTailCheck;
							}
							if (!found) {
								final String op = "no tail for itemGuid, itemGuid: " + FormatSAPI.jsObject(recordItemGuid);
								++context.errors;
								if (!purgeRecord && context.isFix && context.isPurge) {
									purgeRecord = true;
									console.sendMessage("will purge an item: " + op);
									continue runSecondaryChecks;
								}
								console.sendMessage(op);
								break rowTailCheck;
							}
							break rowTailCheck;
						}
						if (purgeRecord && context.isFix && context.isPurge) {
							final String op = " purgable itemGuid record, luid: " + recordItemGuid.key1_luid6;
							WorkerBdbj.writeInt(context.byteBuffer, 0, recordItemGuid.key0_azimuth4);
							WorkerBdbj.writeLongAsLow6(context.byteBuffer, 4, recordItemGuid.key1_luid6);
							key.setData(context.byteBuffer, 0, 4 + 6);
							context.internPurgeRecord(context.local.dbItemGuid, key, op);
							break mainTableCheck;
						}
						break mainTableCheck;
					}
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
		
		return CheckScanItemGuidRecords.check(context)
			? Boolean.TRUE
			: Boolean.FALSE;
	}
	
}
