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

final class CheckScanTreeUsageRecords //
		implements
			Function<CheckContext, Boolean> {
	
	public static final boolean check(final CheckContext context) {
		
		final RowTree recordUsageTree = new RowTree();
		final RowTreeUsage recordUsage = new RowTreeUsage();
		
		final Console console = Exec.currentProcess().getConsole();
		
		final DatabaseEntry key = context.key;
		final DatabaseEntry value = context.value;
		
		/** try ( **/
		final ForwardCursor forwardCursor = context.forwardCursor(TableDefinition.TREE_USAGE);
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
						RowTreeUsage.materializeRowTreeUsage(recordUsage, key.getData());
					} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
						final String op = "invalid treeUsage record, luid" + recordUsage.key1_luid6 + ", e: " + FormatSAPI.plainTextDescribe(e);
						++context.errors;
						/** main check table: purging of corrupted records is the default
						 * behavoir **/
						if (context.isFix && (context.isPurge || !context.isRecover)) {
							context.internPurgeRecord(context.local.dbTreeUsage, key, op);
							break mainTableCheck;
						}
						console.sendMessage(op);
						break mainTableCheck;
					}
					if (context.isVerbose) {
						console.sendMessage("checking: " + FormatSAPI.jsObject(recordUsage));
					}
					rowTreeCheck : {
						boolean found = false;
						try {
							found = RowTree.findRowTreeByKey(//
									recordUsageTree,
									context.cursorTree(),
									key,
									value,
									recordUsage.key1_luid6,
									recordUsage.key2_keyX//
							);
						} catch (final IndexOutOfBoundsException | IllegalArgumentException e) {
							console.sendMessage("invalid tree record, treeUsage key: " + FormatSAPI.jsObject(recordUsage) + ", e: " + FormatSAPI.plainTextDescribe(e));
							found = false;
						}
						if (found) {
							break rowTreeCheck;
						}
						final Cursor cursorTreeUsage = context.cursorTreeUsage();
						final String op = //
								"orphaned treeUsage record" + //
										", treeUsage key: " + FormatSAPI.jsObject(recordUsage)//
						;
						if (context.isFix && context.isPurge) {
							RowTreeUsage.setupTreeUsageKey(//
									key,
									context.byteBuffer,
									recordUsage.key0_targetX,
									recordUsage.key1_luid6,
									recordUsage.key2_keyX//
							);
							if (null == cursorTreeUsage.get(key, null, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN)) {
								console.sendMessage("error purging (not found) " + op);
								++context.errors;
								break rowTreeCheck;
							}
							if (null == cursorTreeUsage.delete(WorkerBdbj.WO_FOR_DROP)) {
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
		
		return CheckScanTreeUsageRecords.check(context)
			? Boolean.TRUE
			: Boolean.FALSE;
	}
	
}
