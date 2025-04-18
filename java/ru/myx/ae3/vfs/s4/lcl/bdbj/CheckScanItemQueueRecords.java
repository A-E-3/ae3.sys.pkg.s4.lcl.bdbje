package ru.myx.ae3.vfs.s4.lcl.bdbj;

import java.util.function.Function;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.ForwardCursor;
import com.sleepycat.je.Get;

import ru.myx.ae3.Engine;
import ru.myx.ae3.console.Console;
import ru.myx.ae3.exec.Exec;
import ru.myx.sapi.FormatSAPI;

final class CheckScanItemQueueRecords //
		implements
			Function<CheckContext, Boolean> {
	
	public static final boolean check(final CheckContext context) {
		
		final RowItem recordItem = new RowItem();
		final RowItemQueue recordItemQueue = new RowItemQueue();
		
		final Console console = Exec.currentProcess().getConsole();
		
		final DatabaseEntry key = context.key;
		final DatabaseEntry value = context.value;
		
		/** try ( **/
		final ForwardCursor forwardCursor = context.forwardCursor(TableDefinition.ITEM_QUEUE);
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
						RowItemQueue.materializeRowItemQueue(recordItemQueue, key.getData());
					} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
						final String op = "invalid itemQueue record" + //
								", luid" + recordItemQueue.key1_luid6 + //
								", e: " + FormatSAPI.plainTextDescribe(e)//
						;
						++context.errors;
						/** main check table: purging of corrupted records is the default
						 * behavoir **/
						if (context.isFix && (context.isPurge || !context.isRecover)) {
							context.internPurgeRecord(context.local.dbItemQueue, key, op);
							break mainTableCheck;
						}
						console.sendMessage(op);
						break mainTableCheck;
					}
					if (context.isVerbose) {
						console.sendMessage("checking: " + FormatSAPI.jsObject(recordItemQueue));
					}
					rowItemCheck : {
						boolean found = false;
						try {
							found = RowItem.findRowItemByKey(//
									recordItem,
									context.cursorItem(),
									key,
									value,
									recordItemQueue.key1_luid6//
							);
						} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
							final String op = "invalid item record, queue: " + FormatSAPI.jsObject(recordItemQueue) + ", e: " + FormatSAPI.plainTextDescribe(e);
							++context.errors;
							/** secondary check table: purging of corrupted records must be
							 * explict **/
							if (context.isFix && context.isPurge) {
								context.internPurgeRecord(context.local.dbItemQueue, key, op);
								break rowItemCheck;
							}
							console.sendMessage(op);
							found = false;
						}
						if (!found) {
							final String op = "orphaned itemQueue record, itemQueue: " + FormatSAPI.jsObject(recordItemQueue);
							++context.errors;
							if (context.isFix && context.isPurge) {
								RowItemQueue.setupItemQueueKey(//
										key,
										context.byteBuffer,
										recordItemQueue.key0_schedule2,
										recordItemQueue.key1_luid6//
								);
								context.internPurgeRecord(context.local.dbItemQueue, key, op);
								break rowItemCheck;
							}
							console.sendMessage(op);
							++context.errors;
							break rowItemCheck;
						}
						{
							if (recordItem.val0_schedule2 != recordItemQueue.key0_schedule2) {
								final RowItemQueue recordQueueOther = new RowItemQueue();
								if (RowItemQueue.findRowItemQueueByKey(//
										recordQueueOther,
										context.cursorItemQueue(),
										key,
										recordItem.val0_schedule2,
										recordItem.key0_luid6//
								)) {
									final String op = "phantom itemQueue record, itemQueue: " + FormatSAPI.jsObject(recordItemQueue);
									++context.errors;
									if (context.isFix && context.isPurge) {
										RowItemQueue.setupItemQueueKey(//
												key,
												context.byteBuffer,
												recordItemQueue.key0_schedule2,
												recordItemQueue.key1_luid6//
										);
										context.internPurgeRecord(context.local.dbItemQueue, key, op);
										break rowItemCheck;
									}
									console.sendMessage(op);
									break rowItemCheck;
								}
								{
									final String op = "incorrect itemQueue record, itemQueue: " + FormatSAPI.jsObject(recordItemQueue) + ", guid="
											+ FormatSAPI.jsObject(recordItem.val1_guidX);
									if (context.isFix) {
										RowItemQueue.setupItemQueueKey(//
												key,
												context.byteBuffer,
												recordItemQueue.key0_schedule2,
												recordItemQueue.key1_luid6//
										);
										context.internPurgeRecord(context.local.dbItemQueue, key, op);
										if (!context.isPurge || context.isRecover) {
											RowItemQueue.serializeRowItemQueue(//
													context.cursorItemQueue(),
													context.byteBuffer,
													key,
													value,
													recordItem.val0_schedule2,
													recordItem.key0_luid6//
											);
											console.sendMessage("replaced " + op);
											++context.fixes;
											break mainTableCheck;
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
						}
						break rowItemCheck;
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
		
		return CheckScanItemQueueRecords.check(context)
			? Boolean.TRUE
			: Boolean.FALSE;
	}
	
}
