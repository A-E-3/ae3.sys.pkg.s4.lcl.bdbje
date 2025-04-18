package ru.myx.ae3.vfs.s4.lcl.bdbj;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.ForwardCursor;
import com.sleepycat.je.Get;
import com.sleepycat.je.Transaction;

import ru.myx.ae3.Engine;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.console.Console;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.reflect.ReflectionEnumerable;
import ru.myx.ae3.reflect.ReflectionExplicit;
import ru.myx.ae3.vfs.s4.common.RepairChecker;
import ru.myx.bdbje.util.ForwardCursorFixedBuffer;
import ru.myx.bdbje.util.ForwardCursorReadAhead;
import ru.myx.util.FifoQueueLinked;
import ru.myx.util.WeakFinalizer;

final class CheckContext //
		extends
			RepairChecker {
	
	static enum Stage {
		
		CHECK_SCAN_ITEM {
			
			@Override
			boolean next(final CheckContext context) {
				
				return CheckScanItemRecords.check(context);
			}
		},
		CHECK_SCAN_ITEM_GUID {
			
			@Override
			boolean next(final CheckContext context) {
				
				return CheckScanItemGuidRecords.check(context);
			}
		},
		CHECK_SCAN_ITEM_QUEUE {
			
			@Override
			boolean next(final CheckContext context) {
				
				return CheckScanItemQueueRecords.check(context);
			}
		},
		CHECK_SCAN_TAIL {
			
			@Override
			boolean next(final CheckContext context) {
				
				return CheckScanTailRecords.check(context);
			}
		},
		CHECK_SCAN_TREE {
			
			@Override
			boolean next(final CheckContext context) {
				
				return CheckScanTreeRecords.check(context);
			}
		},
		CHECK_SCAN_TREE_INDEX {
			
			@Override
			boolean next(final CheckContext context) {
				
				return CheckScanTreeIndexRecords.check(context);
			}
		},
		CHECK_SCAN_TREE_USAGE {
			
			@Override
			boolean next(final CheckContext context) {
				
				return CheckScanTreeUsageRecords.check(context);
			}
		},
		CHECK_DONE {
			
			@Override
			boolean next(final CheckContext context) {
				
				return false;
			}
		},;
		
		abstract boolean next(CheckContext context);
	}
	
	static final int BATCH_LIMIT = 512;
	
	private static void finalizeStatic(final CheckContext x) {
		
		if (x == null) {
			return;
		}
		final ForwardCursor cursor = x.forwardCursor;
		if (cursor != null) {
			// try only once
			x.forwardCursor = null;
			try {
				cursor.close();
			} catch (final Throwable t) {
				// ignore
			}
		}
	}
	
	@ReflectionExplicit
	@ReflectionEnumerable
	public boolean isSlow = false;
	
	/** read limit in main table rows */
	@ReflectionExplicit
	@ReflectionEnumerable
	public int limitRows = 0;
	
	/** time limit in seconds */
	@ReflectionExplicit
	@ReflectionEnumerable
	public long limitTime = 0L;
	
	@ReflectionExplicit
	@ReflectionEnumerable
	public boolean isSeekAzimuth = false;
	
	@ReflectionExplicit
	@ReflectionEnumerable
	public boolean isSeekLuid = false;
	
	@ReflectionExplicit
	@ReflectionEnumerable
	public boolean isSeekSchedule = false;
	
	final byte[] azimuthKey = new byte[]{
			0, 0, 0, 0
	};
	
	final byte[] luidKey = new byte[]{
			0, 0, 0, 0, 0, 0
	};
	
	final byte[] scheduleKey = new byte[]{
			0, 0
	};
	
	Stage stage = null;
	
	private ForwardCursor forwardCursor = null;
	
	BdbjLocalS4 local;
	
	int left;
	long stop;
	
	long stageStartedAt = -1L;
	
	final byte[] byteBuffer = new byte[2048];
	
	final DatabaseEntry key = new DatabaseEntry();
	
	final DatabaseEntry value = new DatabaseEntry();
	
	private Cursor cursorItem;
	
	private Cursor cursorItemGuid;
	
	private Cursor cursorItemQueue;
	
	private Cursor cursorTail;
	
	private Cursor cursorTree;
	
	private Cursor cursorTreeUsage;
	
	private Cursor cursorTreeIndex;
	
	private Transaction txn;
	
	private final FifoQueueLinked<Stage> workload;
	
	CheckContext(final BdbjLocalS4 local) {
		
		this.local = local;
		this.workload = new FifoQueueLinked<>();
		WeakFinalizer.register(this, CheckContext::finalizeStatic);
	}
	
	@Override
	public void close() {
		
		super.close();
		this.stage = null;
		while (this.workload.pollFirst() != null) {
			//
		}
		final ForwardCursor cursor = this.forwardCursor;
		if (cursor != null) {
			// try only once
			this.forwardCursor = null;
			cursor.close();
		}
	}
	
	Cursor cursorItem() {
		
		if (this.cursorItem == null) {
			return this.cursorItem = this.local.dbItem.openCursor(this.txn(), CursorConfig.READ_COMMITTED);
		}
		return this.cursorItem;
	}
	
	Cursor cursorItemGuid() {
		
		if (this.cursorItemGuid == null) {
			return this.cursorItemGuid = this.local.dbItemGuid.openCursor(this.txn(), CursorConfig.READ_COMMITTED);
		}
		return this.cursorItemGuid;
	}
	
	Cursor cursorItemQueue() {
		
		if (this.cursorItemQueue == null) {
			return this.cursorItemQueue = this.local.dbItemQueue.openCursor(this.txn(), CursorConfig.READ_COMMITTED);
		}
		return this.cursorItemQueue;
	}
	
	Cursor cursorTail() {
		
		if (this.cursorTail == null) {
			return this.cursorTail = this.local.dbTail.openCursor(this.txn(), CursorConfig.READ_COMMITTED);
		}
		return this.cursorTail;
	}
	
	Cursor cursorTree() {
		
		if (this.cursorTree == null) {
			return this.cursorTree = this.local.dbTree.openCursor(this.txn(), CursorConfig.READ_COMMITTED);
		}
		return this.cursorTree;
	}
	
	Cursor cursorTreeIndex() {
		
		if (this.cursorTreeIndex == null) {
			return this.cursorTreeIndex = this.local.dbTreeIndex.openCursor(this.txn(), CursorConfig.READ_COMMITTED);
		}
		return this.cursorTreeIndex;
	}
	
	Cursor cursorTreeUsage() {
		
		if (this.cursorTreeUsage == null) {
			return this.cursorTreeUsage = this.local.dbTreeUsage.openCursor(this.txn(), CursorConfig.READ_COMMITTED);
		}
		return this.cursorTreeUsage;
	}
	
	ForwardCursor forwardCursor(final TableDefinition table) {
		
		/** caution: assuming it is closed in time and corresponding to table we are requesting! **/
		if (this.forwardCursor != null) {
			return this.forwardCursor;
		}
		
		byte[] startKey = null;
		switch (table) {
			case ITEM :
				if (this.isSeekLuid) {
					startKey = this.luidKey;
				}
				break;
			case ITEM_GUID :
				if (this.isSeekAzimuth) {
					startKey = this.azimuthKey;
				}
				break;
			case ITEM_QUEUE :
				if (this.isSeekSchedule) {
					startKey = this.scheduleKey;
				}
				break;
			case TAIL :
				if (this.isSeekLuid) {
					startKey = this.luidKey;
				}
				break;
			case TREE :
				if (this.isSeekLuid) {
					startKey = this.luidKey;
				}
				break;
			case TREE_INDEX :
				break;
			case TREE_USAGE :
				break;
			default :
				break;
		}
		
		return //
		this.forwardCursor = this.isSlow
			? (ForwardCursor) new ForwardCursorFixedBuffer(table.getDatabase(this.local), startKey)
			: new ForwardCursorReadAhead(table.getDatabase(this.local), startKey)//
		;
	}
	
	public String getProgress() {
		
		return this.stage.name();
	}
	
	@ReflectionEnumerable
	@ReflectionExplicit
	public String getStage() {
		
		final Stage stage = this.stage;
		return stage == null
			? "null"
			: stage.name();
	}
	
	boolean internPurgeRecord(final Cursor cursor, final DatabaseEntry key, final String op) {
		
		final Console console = Exec.currentProcess().getConsole();
		if (null == cursor.get(key, null, Get.SEARCH, WorkerBdbj.RO_FOR_DROP)) {
			console.sendMessage("error purging (not found) " + op);
			++this.warnings;
			return false;
		}
		if (null == cursor.delete(WorkerBdbj.WO_FOR_DROP)) {
			console.sendMessage("error purging (already deleted) " + op);
			++this.warnings;
			return false;
		}
		console.sendMessage("purged " + op);
		++this.fixes;
		return true;
	}
	
	boolean internPurgeRecord(final Database database, final DatabaseEntry key, final String op) {
		
		assert op != null;
		assert op.length() > 1;
		
		final Console console = Exec.currentProcess().getConsole();
		if (null == database.delete(this.txn(), key, WorkerBdbj.WO_FOR_DROP)) {
			if (this.isVerbose) {
				console.sendMessage("error purging (disappeared, warning) " + op);
			}
			++this.warnings;
			return false;
		}
		console.sendMessage(
				(op.startsWith(" ")
					? " purged:"
					: "purged: ") + op);
		++this.fixes;
		return true;
	}
	
	@Override
	public boolean next() {
		
		if (this.stage == null) {
			this.stage = this.workload.pollFirst();
			if (this.stage == null) {
				Exec.currentProcess().getConsole().sendMessage("s4 check: all stages finished, check done.");
				return false;
			}
			this.stageStartedAt = System.currentTimeMillis();
			this.stop = this.limitTime != 0L
				? Engine.fastTime() + this.limitTime * 1000
				: Engine.fastTime() + 24 * 60 * 60 * 1000;
			this.left = this.limitRows;
			Exec.currentProcess().getConsole().sendMessage("s4 check stage: init " + this.stage);
		}
		if (!this.isAll && this.fixes > 1000) {
			Exec.currentProcess().getConsole().sendMessage("s4 check: too many fixes, stopping, use --all option to have unlimited fixes: ");
			this.close();
			return false;
		}
		try {
			if (this.stage.next(this)) {
				return true;
			}
		} finally {
			this.nextBatch();
		}
		if (this.forwardCursor != null) {
			this.forwardCursor.close();
			this.forwardCursor = null;
		}
		{
			Exec.currentProcess().getConsole()
					.sendMessage("s4 check stage: done " + this.stage + ", took: " + Format.Exact.toPeriod(System.currentTimeMillis() - this.stageStartedAt));
			this.stage = null;
		}
		return true;
	}
	
	void nextBatch() {
		
		{
			final Cursor cursor = this.cursorItem;
			if (cursor != null) {
				this.cursorItem = null;
				cursor.close();
			}
		}
		{
			final Cursor cursor = this.cursorItemGuid;
			if (cursor != null) {
				this.cursorItemGuid = null;
				cursor.close();
			}
		}
		{
			final Cursor cursor = this.cursorItemQueue;
			if (cursor != null) {
				this.cursorItemQueue = null;
				cursor.close();
			}
		}
		{
			final Cursor cursor = this.cursorTail;
			if (cursor != null) {
				this.cursorTail = null;
				cursor.close();
			}
		}
		{
			final Cursor cursor = this.cursorTree;
			if (cursor != null) {
				this.cursorTree = null;
				cursor.close();
			}
		}
		{
			final Cursor cursor = this.cursorTreeIndex;
			if (cursor != null) {
				this.cursorTreeIndex = null;
				cursor.close();
			}
		}
		{
			final Cursor cursor = this.cursorTreeUsage;
			if (cursor != null) {
				this.cursorTreeUsage = null;
				cursor.close();
			}
		}
		{
			final Transaction txn = this.txn;
			if (txn != null) {
				this.txn = null;
				txn.commit();
			}
		}
	}
	
	@Override
	public void setup(final BaseObject properties) {
		
		if (this.stage != null) {
			throw new IllegalStateException("Setup is allowed only during initial stage, stage: " + this.stage);
		}
		super.setup(properties);

		/** reset **/
		this.isSeekAzimuth = false;
		this.isSeekLuid = false;
		this.limitRows = 0;
		this.limitTime = 0L;
		
		final boolean isRand = Base.getBoolean(properties, "rand", false);
		final boolean isSome = Base.getBoolean(properties, "some", false) || isRand;
		
		this.isSlow = Base.getBoolean(properties, "slow", false);
		
		if (isRand) {
			this.isSeekAzimuth = true;
			this.isSeekLuid = true;
			this.isSeekSchedule = true;
			WorkerBdbj.writeInt(this.azimuthKey, 0, Engine.createRandom());
			// WorkerBdbj.writeLongAsLow6(this.luidKey, 0, Engine.createRandomLong());
			WorkerBdbj.writeLongAsLow6(this.luidKey, 0, (long) (Engine.createRandomDouble() * this.local.getSequenceLuid()));
			WorkerBdbj.writeShort(this.scheduleKey, 0, (short) Engine.createRandom());
		}
		
		if (isSome) {
			this.limitRows = 65536;
			this.limitTime = 5 * 60;
		}

		final String limitRows = Base.getString(properties, "limit-rows", "").trim();
		if (limitRows.length() > 0) {
			this.limitRows = Integer.parseInt(limitRows);
		}

		final String limitTime = Base.getString(properties, "limit-time", "").trim();
		if (limitTime.length() > 0) {
			this.limitTime = Long.parseLong(limitTime);
		}
		
		final String startAzimuth = Base.getString(properties, "start-azimuth", "").trim();
		if (startAzimuth.length() > 0) {
			this.isSeekAzimuth = true;
			WorkerBdbj.writeInt(this.azimuthKey, 0, Integer.parseInt(startAzimuth));
		}
		
		final String startLuid = Base.getString(properties, "start-luid", "").trim();
		if (startLuid.length() > 0) {
			this.isSeekLuid = true;
			WorkerBdbj.writeLongAsLow6(this.luidKey, 0, Long.parseLong(startLuid));
		}
		
		final String startSchedule = Base.getString(properties, "start-schedule", "").trim();
		if (startSchedule.length() > 0) {
			this.isSeekSchedule = true;
			WorkerBdbj.writeShort(this.scheduleKey, 0, (short) Long.parseLong(startSchedule));
		}
		
		{
			boolean defaultWorkload = true;
			if (Base.getBoolean(properties, "scan-guid", false)) {
				this.workload.offerLast(Stage.CHECK_SCAN_ITEM_GUID);
				defaultWorkload = false;
			}
			if (Base.getBoolean(properties, "scan-item", false)) {
				this.workload.offerLast(Stage.CHECK_SCAN_ITEM);
				defaultWorkload = false;
			}
			if (Base.getBoolean(properties, "scan-queue", false)) {
				this.workload.offerLast(Stage.CHECK_SCAN_ITEM_QUEUE);
				defaultWorkload = false;
			}
			if (Base.getBoolean(properties, "scan-tail", false)) {
				this.workload.offerLast(Stage.CHECK_SCAN_TAIL);
				defaultWorkload = false;
			}
			if (Base.getBoolean(properties, "scan-tree", false)) {
				this.workload.offerLast(Stage.CHECK_SCAN_TREE);
				defaultWorkload = false;
			}
			if (Base.getBoolean(properties, "scan-index", false)) {
				this.workload.offerLast(Stage.CHECK_SCAN_TREE_INDEX);
				defaultWorkload = false;
			}
			if (Base.getBoolean(properties, "scan-usage", false)) {
				this.workload.offerLast(Stage.CHECK_SCAN_TREE_USAGE);
				defaultWorkload = false;
			}
			if (defaultWorkload) {
				this.workload.offerLast(Stage.CHECK_SCAN_ITEM_GUID);
				this.workload.offerLast(Stage.CHECK_SCAN_ITEM);
				this.workload.offerLast(Stage.CHECK_SCAN_ITEM_QUEUE);
				this.workload.offerLast(Stage.CHECK_SCAN_TAIL);
				this.workload.offerLast(Stage.CHECK_SCAN_TREE);
				this.workload.offerLast(Stage.CHECK_SCAN_TREE_INDEX);
				this.workload.offerLast(Stage.CHECK_SCAN_TREE_USAGE);
			}
		}
		this.workload.offerLast(Stage.CHECK_DONE);
	}
	
	Transaction txn() {
		
		if (this.txn == null) {
			return this.txn = this.local.environment.beginTransaction(null, this.local.transactionConfig);
		}
		return this.txn;
	}
}
