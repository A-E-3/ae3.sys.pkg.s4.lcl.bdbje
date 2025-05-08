package ru.myx.ae3.vfs.s4.lcl.bdbj;

import java.io.File;
import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.ExceptionEvent;
import com.sleepycat.je.ExceptionListener;
import com.sleepycat.je.Get;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.ProgressListener;
import com.sleepycat.je.Put;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.RecoveryProgress;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Act;
import ru.myx.ae3.base.BaseList;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.binary.Transfer;
import ru.myx.ae3.e4.act.Manager.Factory.TYPE;
import ru.myx.ae3.e4.act.ProcessExecutionType;
import ru.myx.ae3.eval.Evaluate;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.help.Create;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.reflect.ReflectionEnumerable;
import ru.myx.ae3.report.Report;
import ru.myx.ae3.report.ReportReceiver;
import ru.myx.ae3.vfs.s4.common.RepairChecker;
import ru.myx.ae3.vfs.s4.common.S4StoreType;
import ru.myx.ae3.vfs.s4.impl.S4TreeDriver;
import ru.myx.ae3.vfs.s4.impl.S4TreeWorker;
import ru.myx.bdbje.upgrade.DbPreUpgrade_4_1;
import ru.myx.bdbje.util.DelayExpungeTask;
import ru.myx.sapi.FormatSAPI;

/** @author myx */
@ProcessExecutionType(type = TYPE.WORKER_POOL)
// @ExecutionManagement(type = TYPE.SINGLE_THREAD)
public class BdbjLocalS4 //
		implements
			S4TreeDriver<RecordBdbj, ReferenceBdbj, Long> {
	
	private static final String BASE_NAME = "bdbj-";
	
	private static final WeakHashMap<String, BdbjLocalS4> INSTANCES = new WeakHashMap<>();
	
	/** Logger */
	static final ReportReceiver LOG = Report.createReceiver("ae3.s4");
	
	private static final long lastLuid(final DatabaseEntry key, final DatabaseEntry value, final Cursor cursor) {
		
		if (null == cursor.get(key, value, Get.LAST, null)) {
			return -1;
		}
		return BdbjLocalS4.readLow6AsLong(key.getData(), 0);
	}
	
	private static final long readLow6AsLong(final byte[] data, final int offset) {
		
		return ((long) (data[offset + 0] & 255) << 40) //
				+ ((long) (data[offset + 1] & 255) << 32) //
				+ ((long) (data[offset + 2] & 255) << 24) //
				+ ((data[offset + 3] & 255) << 16) //
				+ ((data[offset + 4] & 255) << 8) //
				+ ((data[offset + 5] & 255) << 0)//
		;
	}
	
	/** internal, for command line utility
	 *
	 * @return */
	public static final Map<String, BdbjLocalS4> internGetInstances() {
		
		final Map<String, BdbjLocalS4> result = Create.tempMap();
		result.putAll(BdbjLocalS4.INSTANCES);
		return result;
	}
	
	private boolean optionDelayExpunge = false;
	
	private File dataFolder;
	
	/** v 3.01 non-inline containers and small binaries **/
	Database dbItem;
	
	Database dbItemGuid;
	
	Database dbItemQueue;
	
	/** v 3.01 small binaries by luid **/
	Database dbTail;
	
	/** TODO: v 3.03 small binaries by guid **/
	Database dbTails;
	
	/** vfs: tree -> parentLuid;linkName
	 *
	 * see cursorTree */
	Database dbTree;
	
	Database dbTreeIndex;
	
	Database dbTreeUsage;
	
	Environment environment;
	
	S4StoreType instanceType;
	
	private final Map<String, Database> knownDatabases;
	
	private long sequenceLuid = 0;
	
	private final Object sequenceLuidLock = new Object();
	
	/** this.environment.beginTransaction( null, this.transactionConfig ); */
	TransactionConfig transactionConfig;
	
	WeakReference<CheckContext> checkContext = null;
	
	/**
	 *
	 */
	public DelayExpungeTask taskDelayExpunge;
	
	/**
	 *
	 */
	public BdbjLocalS4() {
		
		this.knownDatabases = new TreeMap<>();
	}
	
	@Override
	public RepairChecker createCheckContext(final BaseObject properties) {
		
		if (this.checkContext == null) {
			final CheckContext result = new CheckContext(this);
			this.checkContext = new WeakReference<>(result);
			result.setup(properties);
			return result;
		}
		{
			final CheckContext check = this.checkContext.get();
			if (check == null) {
				final CheckContext result = new CheckContext(this);
				this.checkContext = new WeakReference<>(result);
				result.setup(properties);
				return result;
			}
			if (check.stage == null) {
				check.setup(properties);
				return check;
			}
		}
		throw new IllegalStateException("Checker is busy");
	}
	
	@Override
	public RecordBdbj createRecordTemplate() {
		
		return new RecordBdbjTemplate();
	}
	
	@Override
	public ReferenceBdbj createReferenceTemplate() {
		
		return new ReferenceBdbj();
	}
	
	@Override
	public S4TreeWorker<RecordBdbj, ReferenceBdbj, Long> createWorker() {
		
		return new WorkerBdbj(this);
	}
	
	/** @return */
	@ReflectionEnumerable
	public String getClassName() {
		
		return this.getClass().getSimpleName();
	}
	
	@Override
	@ReflectionEnumerable
	public String getKey() {
		
		return "bdbj";
	}
	
	/** @return */
	@ReflectionEnumerable
	public long getSequenceLuid() {
		
		return this.sequenceLuid;
	}
	
	/** @return */
	@ReflectionEnumerable
	public String getStorageLocation() {
		
		return this.dataFolder.getAbsolutePath();
	}
	
	/** clean log, optional.
	 *
	 * @return non zero if something was achieved */
	public int internStorageClean() {
		
		return this.environment.cleanLog();
	}
	
	/** compress, optional. */
	public void internStorageCompress() {
		
		this.environment.compress();
	}
	
	/** force checkpoint, optional. */
	public void internStorageForce() {
		
		final CheckpointConfig force = new CheckpointConfig();
		force.setMinimizeRecoveryTime(false);
		force.setForce(true);
		this.environment.checkpoint(force);
	}
	
	/** @return stats object */
	public EnvironmentStats internStorageStats() {
		
		final StatsConfig config = new StatsConfig();
		config.setFast(false);
		return this.environment.getStats(config);
	}
	
	/** sync, optional. Used by manual CLI command. */
	public void internStorageSync() {
		
		this.environment.sync();
	}
	
	/** @param operation
	 * @param tableName
	 * @param limit
	 * @param exactKey
	 * @param startKey
	 * @param prefixKey
	 * @param stopKey
	 * @return */
	public BaseList<BaseObject> internStorageTableRangeOperation(//
			final String operation,
			final String tableName,
			final int limit,
			final String exactKey,
			final String prefixKey,
			final String startKey,
			final String stopKey) {
		
		final boolean doDelete;
		switch (operation) {
			case "select" :
				doDelete = false;
				break;
			case "delete" :
				doDelete = true;
				break;
			default :
				throw new UnsupportedOperationException("Unsupported operation: " + operation);
		}
		
		final TableDefinition definition = TableDefinition.findTable(tableName);
		final BaseList<BaseObject> result = BaseObject.createArray();
		
		int left = limit;
		
		final byte[] exactMatch = definition.setupRangeKeyString(exactKey);
		final byte[] prefixMatch = definition.setupRangeKeyString(prefixKey);
		final byte[] startPrefixMatch = definition.setupRangeKeyString(startKey);
		final byte[] stopPrefixMatch = definition.setupRangeKeyString(stopKey);
		
		final int prefixMatchLength = prefixMatch == null
			? -1
			: prefixMatch.length;
		final int exactMatchLength = exactMatch == null
			? -1
			: exactMatch.length;
		final int stopMatchLength = stopPrefixMatch == null
			? -1
			: stopPrefixMatch.length;
		
		final DatabaseEntry key = new DatabaseEntry();
		final DatabaseEntry value = new DatabaseEntry();
		
		key.setData(
				null != startPrefixMatch
					? startPrefixMatch
					: null != exactMatch
						? exactMatch
						: null != prefixMatch
							? prefixMatch
							: new byte[0] //
		);
		
		final ReadOptions readOptions = doDelete
			? WorkerBdbj.RO_FOR_DROP
			: WorkerBdbj.RO_FOR_SCAN;
		
		final Transaction txn = doDelete
			? this.environment.beginTransaction(null, this.transactionConfig)
			: null;
		try {
			cursor : try (Cursor cursor = definition.openCursor(this, txn)) {
				OperationResult status = cursor.get(key, value, Get.SEARCH_GTE, readOptions);
				for (; left-- > 0;) {
					if (null == status) {
						break cursor;
					}
					final byte[] keyData = key.getData();
					if (null != exactMatch) {
						final int keyDataLength = keyData.length;
						if (keyDataLength != exactMatchLength || !WorkerBdbj.compareBytes(keyData, 0, exactMatch, 0, exactMatchLength)) {
							break cursor;
						}
					}
					if (null != prefixMatch) {
						final int keyDataLength = keyData.length;
						if (keyDataLength < prefixMatchLength || !WorkerBdbj.compareBytes(keyData, 0, prefixMatch, 0, prefixMatchLength)) {
							break cursor;
						}
					}
					if (null != stopPrefixMatch) {
						final int keyDataLength = keyData.length;
						if (WorkerBdbj.compareBytes(keyData, 0, stopPrefixMatch, 0, Math.min(keyDataLength, stopMatchLength))) {
							break cursor;
						}
					}
					result.add(definition.rowMaterialize(keyData, value.getData()));
					if (doDelete) {
						cursor.delete(WorkerBdbj.WO_FOR_DROP);
					}
					status = cursor.get(key, value, Get.NEXT, readOptions);
				}
			}
		} finally {
			if (txn != null) {
				txn.commit();
			}
		}
		return result;
	}
	
	/** @param tableName
	 * @return */
	public long internStorageTableRecordCount(final String tableName) {
		
		{
			final Database db = this.knownDatabases.get(tableName);
			if (db != null) {
				return db.count();
			}
		}
		{
			final DatabaseConfig config = new DatabaseConfig();
			config.setAllowCreate(false);
			config.setExclusiveCreate(false);
			config.setReadOnly(true);
			config.setUseExistingConfig(true);
			try {
				try (final Database db = this.environment.openDatabase(null, tableName, config)) {
					final long count = db.count();
					return count;
				}
			} catch (final DatabaseException e) {
				return -1;
			} catch (final IllegalArgumentException e) {
				return -1;
			}
		}
	}
	
	/** @return */
	public List<String> internStorageTables() {
		
		return this.environment.getDatabaseNames();
	}
	
	/** @param tableName
	 * @param description */
	public void internStorageTableStore(final String tableName, final String description) {
		
		final TableDefinition definition = TableDefinition.findTable(tableName);
		final BaseObject descriptionObject = Evaluate.evaluateObject('(' + description + ')', Exec.currentProcess(), null);
		final Transaction txn = this.environment.beginTransaction(null, this.transactionConfig);
		try {
			definition.storeRecord(this, txn, descriptionObject);
		} finally {
			txn.commit();
		}
	}
	
	/** The cleaned '*.jdb' files are not deleted immediately but moved to the 'deleted' folder and
	 * deleted when they are more than 24 hours old.
	 *
	 * @param enable */
	public void setDelayExpungeOption(final boolean enable) {
		
		this.optionDelayExpunge = enable;
	}
	
	@Override
	public void setup(final S4StoreType type) throws Exception {
		
		this.instanceType = type;
		final File folder = new File(Engine.PATH_PRIVATE, "data");
		this.dataFolder = new File(folder, BdbjLocalS4.BASE_NAME + type.toString());
	}
	
	@Override
	public void start() throws Exception {
		
		this.dataFolder.mkdirs();
		
		/** TODO: ? */
		final boolean transactional = !this.instanceType.allowTruncate() || true;
		
		retry : for (;;) {
			System.out.println("S4VFS-IMPL: BDBJE: creating environment...");
			
			// TODO:
			// https://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/EnvironmentConfig.html
			
			final EnvironmentConfig config = new EnvironmentConfig();
			config.setAllowCreate(true);
			config.setLockTimeout(120L, TimeUnit.SECONDS);
			config.setReadOnly(false);
			config.setLocking(true); // should be true for auto-cleaner
			config.setTransactional(transactional);
			config.setTxnTimeout(900L, TimeUnit.SECONDS);
			config.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
			config.setSharedCache(true);
			config.setCacheMode(CacheMode.DEFAULT);
			config.setCachePercent(this.instanceType.defaultCachePercent());
			
			// http://docs.oracle.com/cd/E17277_02/html/java/com/sleepycat/je/EnvironmentConfig.html
			
			config.setConfigParam(
					EnvironmentConfig.ADLER32_CHUNK_SIZE, //
					Engine.MODE_SIZE
						? "8192"
						: Engine.MODE_SPEED
							? "65536"
							: "16384");
			
			config.setConfigParam(EnvironmentConfig.CHECKPOINTER_HIGH_PRIORITY, "true");
			config.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, "134217728"); // 67108864
			config.setConfigParam(EnvironmentConfig.CHECKPOINTER_WAKEUP_INTERVAL, "67 s");
			
			config.setConfigParam(
					EnvironmentConfig.ENV_BACKGROUND_READ_LIMIT,
					Engine.MODE_SIZE
						? "128"
						: Engine.MODE_SPEED
							? "2048"
							: "1024"//
			);
			config.setConfigParam(
					EnvironmentConfig.ENV_BACKGROUND_WRITE_LIMIT,
					Engine.MODE_SIZE
						? "64"
						: Engine.MODE_SPEED
							? "1024"
							: "512"//
			);
			config.setConfigParam(EnvironmentConfig.ENV_BACKGROUND_SLEEP_INTERVAL, "1 ms");
			
			config.setConfigParam(EnvironmentConfig.EVICTOR_CORE_THREADS, "1");
			config.setConfigParam(EnvironmentConfig.EVICTOR_MAX_THREADS, String.valueOf(3 + Math.min(Engine.PARALLELISM, 4)));
			config.setConfigParam(EnvironmentConfig.EVICTOR_KEEP_ALIVE, "17 s");
			
			config.setConfigParam(
					EnvironmentConfig.CLEANER_THREADS,
					String.valueOf(//
							Math.min(//
									/** Not more than actual number of CPU cores available */
									Engine.PARALLELISM,
									/** Number depends on modes, core count and limits */
									/** https://docs.google.com/spreadsheets/d/1JpVkPTUcfkstx4rzsHiYcq1J8WX0Ti4Oe2Wl3gX3ngE/edit#gid=0 */
									Engine.MODE_SIZE || !this.optionDelayExpunge
										/** 1-3CPU: 1, 4-7CPU: 2, 8-11CPU: 3, MAX: 8 */
										? Math.max(1, Math.min(1 + Engine.PARALLELISM * 10 / 40, 8))
										: Engine.MODE_SPEED
											/** 1 + ( 1-5:5, 6:6, 7:7, 8-9:8 ), <br/>
											 * MAX:32 */
											? 1 + Math.max(5, Math.min(2 + Engine.PARALLELISM * 30 / 40, 31))
											/** 1 + ( 1-3:3, 4-5:4, 6-7:5, 8-9:7, 10-11:8 ), <br/>
											 * MAX:16 */
											: 1 + Math.max(3, Math.min(2 + Engine.PARALLELISM * 25 / 40, 15))//
							)//
					)//
			);
			
			if (this.optionDelayExpunge) {
				final File deletedFolder = new File(this.dataFolder, "deleted");
				this.taskDelayExpunge = new DelayExpungeTask(deletedFolder);
				System.out.println("S4VFS-IMPL: 'delay-expunge' enabled, 'deleted' folder location is: " + deletedFolder.getAbsolutePath());
				config.setConfigParam(EnvironmentConfig.CLEANER_EXPUNGE, "false");
				config.setConfigParam(EnvironmentConfig.CLEANER_USE_DELETED_DIR, "true");
			}
			
			/** MIN_FREE_DISK: 3 Gb **/
			config.setConfigParam(EnvironmentConfig.FREE_DISK, String.valueOf(3L * 1024L * 1024L * 1024L));
			
			config.setConfigParam(EnvironmentConfig.CLEANER_MIN_AGE, "3");
			config.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION, "47"); // 47
			config.setConfigParam(EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION, "13"); // 23

			/** upgrade old files to current version */
			config.setConfigParam(EnvironmentConfig.CLEANER_UPGRADE_TO_LOG_VERSION, "-1");
			config.setConfigParam(
					EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE, //
					String.valueOf(
							Engine.MODE_SIZE
								? Math.min(Transfer.BUFFER_MEDIUM, 4096)
								: Engine.MODE_SPEED
									? Math.max(Transfer.BUFFER_MEDIUM, 32768)
									: Math.min(Transfer.BUFFER_MEDIUM, 16384)//
					)//
			);
			
			config.setConfigParam(
					EnvironmentConfig.LOCK_N_LOCK_TABLES, //
					Engine.MODE_SIZE
						? "7"
						: Engine.MODE_SPEED
							? "67"
							: "37"//
			);
			config.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "268435456"); // 67108864
			
			config.setConfigParam(
					EnvironmentConfig.FILE_LOGGING_LEVEL, //
					Report.MODE_DEVEL
						? "FINER"
						: Report.MODE_ASSERT | Report.MODE_DEBUG
							? "INFO"
							: "WARNING"//
			);
			
			// config.setConfigParam( EnvironmentConfig.LOG_CHECKSUM_READ,
			// "false" );
			
			config.setConfigParam(
					EnvironmentConfig.LOG_FAULT_READ_SIZE, //
					Engine.MODE_SIZE
						? "2048"
						: "4096"//
			);
			
			final String name = BdbjLocalS4.BASE_NAME + this.instanceType.toString();
			
			config.setRecoveryProgressListener(new ProgressListener<RecoveryProgress>() {
				
				@Override
				public boolean progress(final RecoveryProgress phase, final long n, final long total) {
					
					System.out.println("S4VFS-IMPL: BDBJE: startup/recovery, phase is " + phase + ", done " + Format.Compact.toDecimal(100.0 * n / total) + "%...");
					return true;
				}
			});
			
			config.setExceptionListener(new ExceptionListener() {
				
				@Override
				public void exceptionThrown(final ExceptionEvent event) {
					
					BdbjLocalS4.LOG.event("BDBJ-LCL", "EXCEPTION:" + name + ':' + event.getThreadName(), Format.Throwable.toText(event.getException()));
				}
			});
			
			config.setLoggingHandler(new Handler() {
				
				@Override
				public void close() throws SecurityException {
					
					BdbjLocalS4.LOG.event("BDBJ-LCL", "MESSAGE:" + name, "Log closed.");
				}
				
				@Override
				public void flush() {
					
					// ignore
				}
				
				@Override
				public void publish(final LogRecord record) {
					
					BdbjLocalS4.LOG.event("BDBJ-LCL", BdbjLocalS4.this.toString(), record.getMessage());
				}
			});
			
			try {
				this.environment = new Environment(this.dataFolder, config);
				break;
			} catch (final EnvironmentFailureException e) {
				if (e.getMessage().contains("DbPreUpgrade_4_1")) {
					System.out.println("S4VFS-IMPL: BDBJE: upgrade required...");
					DbPreUpgrade_4_1.upgrade(this.dataFolder);
					continue retry;
				}
				throw e;
			}
		}
		{
			if (transactional) {
				final TransactionConfig config = new TransactionConfig();
				config.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
				this.transactionConfig = config;
			} else {
				this.transactionConfig = null;
			}
			{
				System.out.println("S4VFS-IMPL: BDBJE: opening 'item' database...");
				final DatabaseConfig config = new DatabaseConfig();
				config.setAllowCreate(true);
				config.setExclusiveCreate(false);
				config.setReadOnly(false);
				config.setTransactional(transactional);
				config.setSortedDuplicates(false);
				// config.setDeferredWrite( true );
				this.dbItem = this.environment.openDatabase(null, "item", config);
				this.knownDatabases.put("item", this.dbItem);
			}
			{
				final DatabaseConfig config = new DatabaseConfig();
				config.setAllowCreate(true);
				config.setExclusiveCreate(false);
				config.setReadOnly(false);
				config.setTransactional(transactional);
				config.setSortedDuplicates(false);
				System.out.println("S4VFS-IMPL: BDBJE: opening 'guid' database...");
				try {
					this.dbItemGuid = this.environment.openDatabase(null, "guid", config);
				} catch (final IllegalArgumentException e) {
					System.out.println("S4VFS-IMPL: BDBJE: opening 'guid' database, in DUP to NO_DUP conversion mode...");
					final DatabaseConfig config2 = config.clone();
					config2.setSortedDuplicates(true);
					byte[] keyLast = new byte[0];
					try {
						try (final Database databaseNew = this.environment.openDatabase(null, "guid.temp", config)) {
							try (final Cursor cursor = databaseNew.openCursor(null, null)) {
								final DatabaseEntry key = new DatabaseEntry();
								final DatabaseEntry value = new DatabaseEntry();
								if (null != cursor.get(key, value, Get.LAST, null)) {
									keyLast = key.getData();
									System.out.println(
											"S4VFS-IMPL: BDBJE: CONV-guid: progress: continue from: "
													+ FormatSAPI.jsObject(TableDefinition.ITEM_GUID.rowMaterialize(keyLast, value.getData())));
								}
							}
						}
						// this.environment.removeDatabase( null, "guid.temp" );
					} catch (final DatabaseNotFoundException ee) {
						// ignore
					}
					try (final Database databaseOld = this.environment.openDatabase(null, "guid", config2);
							final Database databaseNew = this.environment.openDatabase(null, "guid.temp", config)) {
						//
						try (final Cursor cursor = databaseOld.openCursor(null, null)) {
							final DatabaseEntry key = new DatabaseEntry(keyLast);
							final DatabaseEntry value = new DatabaseEntry();
							int recCopied = 0, recSkipped = 0;
							OperationResult status = cursor.get(key, value, Get.SEARCH_GTE, WorkerBdbj.RO_FOR_SCAN);
							for (int count = 0;;) {
								if (null == status) {
									break;
								}
								final byte[] keyNew = key.getData();
								if (keyLast.length != 0 && WorkerBdbj.compareBytes(keyNew, 0, keyLast, 0, 4 + 6)) {
									++recSkipped;
								} else {
									if (null == databaseNew.put(null, key, value, Put.OVERWRITE, WorkerBdbj.WO_FOR_SAVE)) {
										throw new IllegalStateException("Failed to copy: status: " + status);
									}
									keyLast = keyNew;
									++recCopied;
								}
								if (++count % 10000 == 0) {
									System.out.println("S4VFS-IMPL: BDBJE: CONV-guid: progress: copied: " + recCopied + ", skipped: " + recSkipped);
								}
								status = cursor.get(key, value, Get.NEXT, WorkerBdbj.RO_FOR_SCAN);
							}
							System.out.println("S4VFS-IMPL: BDBJE: CONV-guid: finished: copied: " + recCopied + ", skipped: " + recSkipped);
						}
					}
					//
					this.environment.removeDatabase(null, "guid");
					this.environment.renameDatabase(null, "guid.temp", "guid");
					
					System.out.println("S4VFS-IMPL: BDBJE: CONV-guid: database replaced, done.");
					//
					this.dbItemGuid = this.environment.openDatabase(null, "guid", config);
				}
				this.knownDatabases.put("guid", this.dbItemGuid);
			}
			{
				System.out.println("S4VFS-IMPL: BDBJE: opening 'queue' secondary database...");
				final DatabaseConfig config = new DatabaseConfig();
				config.setAllowCreate(true);
				config.setExclusiveCreate(false);
				config.setReadOnly(false);
				config.setTransactional(transactional);
				config.setSortedDuplicates(false);
				this.dbItemQueue = this.environment.openDatabase(null, "queue", config);
				this.knownDatabases.put("queue", this.dbItemQueue);
			}
			{
				System.out.println("S4VFS-IMPL: BDBJE: opening 'tree' database...");
				final DatabaseConfig config = new DatabaseConfig();
				config.setAllowCreate(true);
				config.setExclusiveCreate(false);
				config.setReadOnly(false);
				config.setTransactional(transactional);
				config.setSortedDuplicates(false);
				config.setKeyPrefixing(true);
				this.dbTree = this.environment.openDatabase(null, "tree", config);
				this.knownDatabases.put("tree", this.dbTree);
			}
			if (this.instanceType.storeIndex()) {
				System.out.println("S4VFS-IMPL: BDBJE: opening 'index' secondary database...");
				final DatabaseConfig config = new DatabaseConfig();
				config.setAllowCreate(true);
				config.setExclusiveCreate(false);
				config.setReadOnly(false);
				config.setTransactional(transactional);
				config.setSortedDuplicates(false);
				config.setKeyPrefixing(true);
				this.dbTreeIndex = this.environment.openDatabase(null, "index", config);
				this.knownDatabases.put("index", this.dbTreeIndex);
			}
			if (this.instanceType.storeUsage()) {
				System.out.println("S4VFS-IMPL: BDBJE: opening 'usage' secondary database...");
				final DatabaseConfig config = new DatabaseConfig();
				config.setAllowCreate(true);
				config.setExclusiveCreate(false);
				config.setReadOnly(false);
				config.setTransactional(transactional);
				config.setSortedDuplicates(false);
				this.dbTreeUsage = this.environment.openDatabase(null, "usage", config);
				this.knownDatabases.put("usage", this.dbTreeUsage);
			}
			{
				System.out.println("S4VFS-IMPL: BDBJE: opening 'tail' database...");
				final DatabaseConfig config = new DatabaseConfig();
				config.setAllowCreate(true);
				config.setExclusiveCreate(false);
				config.setReadOnly(false);
				config.setTransactional(transactional);
				config.setSortedDuplicates(false);
				this.dbTail = this.environment.openDatabase(null, "tail", config);
				this.knownDatabases.put("tail", this.dbTail);
			}
		}
		
		final Transaction txn = transactional
			? this.environment.beginTransaction(null, this.transactionConfig)
			: null;
		{
			System.out.println("S4VFS-IMPL: BDBJE: reading latest sequence luid...");
			long greatestLuid = 0L;
			final DatabaseEntry key = new DatabaseEntry();
			final DatabaseEntry value = new DatabaseEntry();
			try (final Cursor cursor = this.dbItem.openCursor(
					txn, //
					CursorConfig.READ_COMMITTED)) {
				final long luid = BdbjLocalS4.lastLuid(key, value, cursor);
				greatestLuid = Math.max(greatestLuid, luid);
				System.out.println(
						"S4VFS-IMPL: BDBJE: 'item' database: " + (luid == -1
							? "empty table"
							: luid + ", max=" + greatestLuid));
			}
			try (final Cursor cursor = this.dbTail.openCursor(
					txn, //
					CursorConfig.READ_COMMITTED)) {
				final long luid = BdbjLocalS4.lastLuid(key, value, cursor);
				greatestLuid = Math.max(greatestLuid, luid);
				System.out.println(
						"S4VFS-IMPL: BDBJE: 'tail' database: " + (luid == -1
							? "empty table"
							: luid + ", max=" + greatestLuid));
			}
			try (final Cursor cursor = this.dbTree.openCursor(
					txn, //
					CursorConfig.READ_COMMITTED)) {
				final long luid = BdbjLocalS4.lastLuid(key, value, cursor);
				greatestLuid = Math.max(greatestLuid, luid);
				System.out.println(
						"S4VFS-IMPL: BDBJE: 'tree' database: " + (luid == -1
							? "empty table"
							: luid + ", max=" + greatestLuid));
			}
			this.sequenceLuid = greatestLuid;
			System.out.println("S4VFS-IMPL: BDBJE: greatest luid is: " + this.sequenceLuid);
		}
		if (txn != null) {
			txn.commit();
		}
		
		BdbjLocalS4.INSTANCES.put(this.instanceType.toString(), this);
		if (this.taskDelayExpunge != null) {
			Act.later(Exec.getRootProcess(), new Runnable() {
				
				@Override
				public void run() {
					
					final DelayExpungeTask task = BdbjLocalS4.this.taskDelayExpunge;
					if (task == null) {
						return;
					}
					try {
						task.run();
					} catch (final Throwable t) {
						t.printStackTrace();
					}
					
					final long delay = task.lastAvail > 0.6
						? 60_000L
						: task.lastAvail > 0.37
							? 30_000L
							: task.lastAvail > 0.07
								? 15_000L
								: 5_000L;
					
					Act.later(Exec.getRootProcess(), this, delay);
					
					System.out.println("S4VFS-IMPL: BDBJE: delete-expunge scheduled to run in " + Format.Compact.toPeriod(delay) + ".");
				}
			}, /* long enough to not run when started with 'env broken' */220_000L);
			System.out.println("S4VFS-IMPL: BDBJE: delete-expunge scheduled to run in 220 seconds.");
		}
		System.out.println("S4VFS-IMPL: BDBJE: done.");
	}
	
	@Override
	public void stop() throws Exception {
		
		if (this.environment == null) {
			return;
		}
		BdbjLocalS4.INSTANCES.remove(this.instanceType.toString());
		
		if (this.taskDelayExpunge != null) {
			this.taskDelayExpunge = null;
		}
		
		if (this.dbTail != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'tail' database...");
			try {
				this.dbTail.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.dbTail = null;
		}
		if (this.dbTreeUsage != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'usage' secondary database...");
			try {
				this.dbTreeUsage.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.dbTreeUsage = null;
		}
		if (this.dbTreeIndex != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'index' secondary database...");
			try {
				this.dbTreeIndex.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.dbTreeIndex = null;
		}
		if (this.dbTree != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'tree' database...");
			try {
				this.dbTree.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.dbTree = null;
		}
		if (this.dbItemQueue != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'queue' secondary database...");
			try {
				this.dbItemQueue.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.dbItemQueue = null;
		}
		if (this.dbItemGuid != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'guid' secondary database...");
			try {
				this.dbItemGuid.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.dbItemGuid = null;
		}
		if (this.dbItem != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'item' database...");
			try {
				this.dbItem.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.dbItem = null;
		}
		if (this.environment != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing environment...");
			try {
				this.environment.sync();
			} catch (final DatabaseException e) {
				// ignore
			}
			try {
				this.environment.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.environment = null;
			this.knownDatabases.clear();
			System.out.println("S4VFS-IMPL: BDBJE: closed.");
		}
	}
	
	@Override
	public long storageCalculate() throws Exception {
		
		long size = 0;
		final File[] files = this.dataFolder.listFiles();
		if (files != null) {
			for (final File file : files) {
				size += file.length();
			}
		}
		return size;
	}
	
	@Override
	public void storageTruncate() throws Exception {
		
		if (!this.instanceType.allowTruncate()) {
			throw new IllegalStateException("Storage type prohibits truncation of storage!");
		}
		final File[] files = this.dataFolder.listFiles();
		if (files != null) {
			for (final File file : files) {
				file.delete();
			}
		}
	}
	
	@Override
	public String toString() {
		
		return "S4Local: BDBJE" + (this.dataFolder == null
			? ""
			: ", data=" + this.dataFolder.getAbsolutePath());
	}
	
	/** @return */
	protected long nextSequenceLuid() {
		
		synchronized (this.sequenceLuidLock) {
			return ++this.sequenceLuid;
		}
	}
}
