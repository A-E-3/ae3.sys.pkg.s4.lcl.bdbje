package ru.myx.ae3.vfs.s4.lcl.bdbj;

import java.util.function.Function;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.ForwardCursor;
import com.sleepycat.je.Get;

import ru.myx.ae3.Engine;
import ru.myx.ae3.console.Console;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.know.Guid;
import ru.myx.sapi.FormatSAPI;

final class CheckScanItemRecords //
		implements
			Function<CheckContext, Boolean> {

	public static final boolean check(final CheckContext context) {

		final RowItem recordItem = new RowItem();
		final RowItemGuid recordItemGuid = new RowItemGuid();
		final RowItemQueue recordItemQueue = new RowItemQueue();

		final Console console = Exec.currentProcess().getConsole();

		final DatabaseEntry key = context.key;
		final DatabaseEntry value = context.value;

		/** try ( **/
		final ForwardCursor forwardCursor = context.forwardCursor(TableDefinition.ITEM);
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
						RowItem.materializeRowItem(//
								recordItem,
								key.getData(),
								value.getData()//
						);
					} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
						final String op = "invalid item record, luid" + recordItem.key0_luid6 + ", e: " + FormatSAPI.plainTextDescribe(e);
						++context.errors;
						/** main check table: purging of corrupted records is the default
						 * behavoir **/
						if (context.isFix && (context.isPurge || !context.isRecover)) {
							context.internPurgeRecord(context.local.dbItem, key, op);
							break mainTableCheck;
						}
						console.sendMessage(op);
						break mainTableCheck;
					}
					if (context.isVerbose) {
						console.sendMessage("checking: " + FormatSAPI.jsObject(recordItem));
					}
					if (!recordItem.val1_guidX.isValid()) {
						console.sendMessage("invalid guid: " + FormatSAPI.jsObject(recordItem));
						++context.errors;
						break mainTableCheck;
					}
					if (recordItem.val1_guidX.isPrimitive() && recordItem.val1_guidX != Guid.GUID_NULL) {
						console.sendMessage("primitive guid: " + FormatSAPI.jsObject(recordItem));
						++context.errors;
						break mainTableCheck;
					}
					if (recordItem.val1_guidX.isInline() && recordItem.val1_guidX != Guid.GUID_NULL) {
						console.sendMessage("inline guid: " + FormatSAPI.jsObject(recordItem));
						++context.errors;
						break mainTableCheck;
					}

					rowItemGuidCheck : {
						final int azimuth = recordItem.val1_guidX.hashCode();
						boolean found = false;
						try {
							found = RowItemGuid.findRowItemGuidByKey(//
									recordItemGuid,
									context.cursorItemGuid(),
									key,
									value,
									azimuth,
									recordItem.key0_luid6//
							);
						} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
							final String op = "invalid itemGuid record, item: " + FormatSAPI.jsObject(recordItem) + ", e: " + FormatSAPI.plainTextDescribe(e);
							++context.errors;
							/** secondary check table: purging of corrupted records must be
							 * explict **/
							if (context.isFix && context.isPurge) {
								context.internPurgeRecord(context.local.dbItemGuid, key, op);
								break rowItemGuidCheck;
							}
							console.sendMessage(op);
							found = false;
						}
						if (!found) {
							if (context.isFix && (!context.isPurge || context.isRecover)) {
								RowItemGuid.serializeRowItemGuid(
										context.cursorItemGuid(),
										context.byteBuffer,
										key,
										value,
										azimuth,
										recordItem.key0_luid6,
										recordItem.val0_schedule2,
										recordItem.val1_guidX);
								console.sendMessage("fixed itemGuid for item, item: " + FormatSAPI.jsObject(recordItem));
								++context.fixes;
								break rowItemGuidCheck;
							}
							if (context.isFix && context.isPurge && !context.isRecover) {
								final String op = "no itemGuid for item record, item: " + FormatSAPI.jsObject(recordItem);
								RowItem.setupItemKey(key, context.byteBuffer, recordItem.key0_luid6);
								context.internPurgeRecord(context.local.dbItem, key, op);
								break rowItemGuidCheck;
							}
							console.sendMessage("no itemGuid for item, item: " + FormatSAPI.jsObject(recordItem));
							++context.errors;
							break rowItemGuidCheck;
						}
						if (recordItem.val0_schedule2 != recordItemGuid.val0_schedule2) {
							if (context.isFix && (!context.isPurge || context.isRecover)) {
								RowItemGuid.serializeRowItemGuid( //
										context.cursorItemGuid(),
										context.byteBuffer,
										key,
										value,
										azimuth,
										recordItem.key0_luid6,
										recordItem.val0_schedule2,
										recordItem.val1_guidX);
								console.sendMessage("fixed invalid schedule in itemGuid for item, item: " + FormatSAPI.jsObject(recordItem));
								++context.fixes;
								break rowItemGuidCheck;
							}
							console.sendMessage("invalid schedule in itemGuid for item, item: " + FormatSAPI.jsObject(recordItem));
							++context.errors;
							break rowItemGuidCheck;
						}
						break rowItemGuidCheck;
					}
					rowItemQueueCheck : {
						boolean found = false;
						try {
							found = RowItemQueue.findRowItemQueueByKey( //
									recordItemQueue,
									context.cursorItemQueue(),
									key,
									recordItem.val0_schedule2,
									recordItem.key0_luid6//
							);
						} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
							final String op = "invalid itemQueue record, item: " + FormatSAPI.jsObject(recordItem) + ", e: " + FormatSAPI.plainTextDescribe(e);
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
						if (!found) {
							if (context.isFix && !context.isPurge) {
								RowItemQueue.serializeRowItemQueue(//
										context.cursorItemQueue(),
										context.byteBuffer,
										key,
										value,
										recordItem.val0_schedule2,
										recordItem.key0_luid6//
								);
								console.sendMessage("fixed itemQueue for item, item: " + FormatSAPI.jsObject(recordItem));
								++context.fixes;
								break rowItemQueueCheck;
							}
							console.sendMessage("no itemQueue for item, item: " + FormatSAPI.jsObject(recordItem));
							++context.errors;
							break rowItemQueueCheck;
						}
						break rowItemQueueCheck;
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

		return CheckScanItemRecords.check(context)
			? Boolean.TRUE
			: Boolean.FALSE;
	}

}
