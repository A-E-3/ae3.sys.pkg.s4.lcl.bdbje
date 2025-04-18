package ru.myx.ae3.vfs.s4.lcl.bdbj;

import java.util.function.Function;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.ForwardCursor;
import com.sleepycat.je.Get;

import ru.myx.ae3.Engine;
import ru.myx.ae3.console.Console;
import ru.myx.ae3.exec.Exec;
import ru.myx.sapi.FormatSAPI;

final class CheckScanTailRecords //
		implements
			Function<CheckContext, Boolean> {
	
	public static final boolean check(final CheckContext context) {
		
		final RowTail recordTail = new RowTail();
		final RowItem recordItem = new RowItem();
		
		final Console console = Exec.currentProcess().getConsole();
		
		final DatabaseEntry key = context.key;
		final DatabaseEntry value = context.value;
		
		/** try ( **/
		final ForwardCursor forwardCursor = context.forwardCursor(TableDefinition.TAIL);
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
						RowTail.materializeRowTail(recordTail, key.getData(), value.getData());
					} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
						final String op = "invalid tail record, luid" + recordTail.key0_luid6 + ", e: " + FormatSAPI.plainTextDescribe(e);
						++context.errors;
						/** main check table: purging of corrupted records is the default
						 * behavoir **/
						if (context.isFix && (context.isPurge || !context.isRecover)) {
							context.internPurgeRecord(context.local.dbTail, key, op);
							break mainTableCheck;
						}
						console.sendMessage(op);
						++context.errors;
						break mainTableCheck;
					}
					if (context.isVerbose) {
						console.sendMessage("checking: " + FormatSAPI.jsObject(recordTail));
					}
					
					/** nothing to check here... yet? **/
					// recordTail.val0_dataX
					
					rowItemCheck : {
						boolean found = false;
						try {
							found = RowItem.findRowItemByKey(//
									recordItem,
									context.cursorItem(),
									key,
									value,
									recordTail.key0_luid6//
							);
						} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
							final String op = "invalid item record, tail key: " + FormatSAPI.jsObject(recordTail) + ", e: " + FormatSAPI.plainTextDescribe(e);
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
						if (found) {
							if (recordItem.val1_guidX.isInline() || !recordItem.val1_guidX.isBinary()) {
								final String op = "mismatching tail record, tail: " + FormatSAPI.jsObject(recordTail);
								++context.errors;
								if (context.isFix && context.isPurge) {
									RowTail.setupTailKey(key, context.byteBuffer, recordTail.key0_luid6);
									context.internPurgeRecord(context.local.dbTail, key, op);
									break rowItemCheck;
								}
								console.sendMessage(op);
								break rowItemCheck;
							}
							break rowItemCheck;
						}
						{
							final String op = "orphaned tail record, tail: " + FormatSAPI.jsObject(recordTail);
							++context.errors;
							if (context.isFix && context.isPurge) {
								RowTail.setupTailKey(key, context.byteBuffer, recordTail.key0_luid6);
								context.internPurgeRecord(context.local.dbTail, key, op);
								break rowItemCheck;
							}
							console.sendMessage(op);
							break rowItemCheck;
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
	
	@Override
	public Boolean apply(final CheckContext context) {
		
		return CheckScanTailRecords.check(context)
			? Boolean.TRUE
			: Boolean.FALSE;
	}
	
}
