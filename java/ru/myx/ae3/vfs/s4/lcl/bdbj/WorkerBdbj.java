package ru.myx.ae3.vfs.s4.lcl.bdbj;

import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.Get;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;
import com.sleepycat.je.ReadOptions;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.TransactionTimeoutException;
import com.sleepycat.je.WriteOptions;

import ru.myx.ae3.Engine;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.know.Guid;
import ru.myx.ae3.report.Report;
import ru.myx.ae3.report.ReportReceiver;
import ru.myx.ae3.vfs.TreeLinkType;
import ru.myx.ae3.vfs.s4.common.RecImpl;
import ru.myx.ae3.vfs.s4.common.RecTemplate;
import ru.myx.ae3.vfs.s4.driver.S4Driver;
import ru.myx.ae3.vfs.s4.impl.S4TreeWorker;
import ru.myx.util.BasicQueue;
import ru.myx.util.FifoQueueLinked;

/** @author myx */
public class WorkerBdbj //
		implements
			S4TreeWorker<RecordBdbj, ReferenceBdbj, Long> {
	
	static final ReadOptions RO_RMW;
	static final ReadOptions RO_FOR_DROP;
	static final ReadOptions RO_FOR_READ;
	static final ReadOptions RO_FOR_SCAN;
	static final ReadOptions RO_FOR_WRITE;
	static final WriteOptions WO_FOR_SAVE;
	static final WriteOptions WO_FOR_READ;
	static final WriteOptions WO_FOR_DROP;
	
	/** Logger */
	static final ReportReceiver LOG = Report.createReceiver("ae3.s4");
	
	static {
		RO_RMW = new ReadOptions();
		WorkerBdbj.RO_RMW.setLockMode(LockMode.RMW);
		
		RO_FOR_DROP = new ReadOptions();
		WorkerBdbj.RO_FOR_DROP.setCacheMode(CacheMode.EVICT_BIN);
		WorkerBdbj.RO_FOR_DROP.setLockMode(LockMode.RMW);
		
		RO_FOR_READ = new ReadOptions();
		WorkerBdbj.RO_FOR_READ.setCacheMode(CacheMode.DEFAULT);
		WorkerBdbj.RO_FOR_READ.setLockMode(LockMode.READ_UNCOMMITTED);
		/** LockMode.READ_COMMITTED not allowed with Cursor methods, use
		 * CursorConfig.setReadCommitted instead. */
		// WorkerBdbj.RO_FOR_READ.setLockMode(LockMode.READ_COMMITTED);
		
		RO_FOR_SCAN = new ReadOptions();
		WorkerBdbj.RO_FOR_SCAN.setCacheMode(CacheMode.UNCHANGED);
		WorkerBdbj.RO_FOR_SCAN.setLockMode(LockMode.READ_UNCOMMITTED);
		/** LockMode.READ_COMMITTED not allowed with Cursor methods, use
		 * CursorConfig.setReadCommitted instead. */
		// WorkerBdbj.RO_FOR_SCAN.setLockMode(LockMode.READ_COMMITTED);
		
		RO_FOR_WRITE = new ReadOptions();
		WorkerBdbj.RO_FOR_WRITE.setCacheMode(CacheMode.DEFAULT);
		WorkerBdbj.RO_FOR_WRITE.setLockMode(LockMode.RMW);
		
		WO_FOR_DROP = new WriteOptions();
		WorkerBdbj.WO_FOR_DROP.setCacheMode(CacheMode.EVICT_BIN);
		
		WO_FOR_SAVE = new WriteOptions();
		WorkerBdbj.WO_FOR_SAVE.setCacheMode(CacheMode.UNCHANGED);
		
		WO_FOR_READ = new WriteOptions();
		WorkerBdbj.WO_FOR_READ.setCacheMode(CacheMode.DEFAULT);
	}
	
	private static final int DUPLICATE_COUNT_LIMIT = 16384;
	
	private static final int DUPLICATE_SCANNED_LIMIT = 2 * 65536;
	
	private static final long DUPLICATE_TIME_LIMIT = 1300L;
	
	static final boolean compareBytes(//
			final byte[] bytes1, //
			final int offset1, //
			final byte[] bytes2, //
			final int offset2, //
			final int length//
	) {
		
		for (int i = 0; i < length; ++i) {
			if (bytes1[offset1 + i] != bytes2[offset2 + i]) {
				return false;
			}
		}
		return true;
	}
	
	static final int readInt(final byte[] data, final int offset) {
		
		return (data[offset + 0] & 255) << 24 //
				| (data[offset + 1] & 255) << 16 //
				| (data[offset + 2] & 255) << 8 //
				| (data[offset + 3] & 255) << 0//
		;
	}
	
	static final long readLow6AsLong(final byte[] data, final int offset) {
		
		return ((long) (data[offset + 0] & 255) << 40) //
				+ ((long) (data[offset + 1] & 255) << 32) //
				+ ((long) (data[offset + 2] & 255) << 24) //
				+ ((data[offset + 3] & 255) << 16) //
				+ ((data[offset + 4] & 255) << 8) //
				+ ((data[offset + 5] & 255) << 0)//
		;
	}
	
	static final long readLow6AsLongReversed(final byte[] data, final int offset) {
		
		return ((long) (data[offset + 5] & 255) << 40) //
				+ ((long) (data[offset + 4] & 255) << 32) //
				+ ((long) (data[offset + 3] & 255) << 24) //
				+ ((data[offset + 2] & 255) << 16) //
				+ ((data[offset + 1] & 255) << 8) //
				+ ((data[offset + 0] & 255) << 0)//
		;
	}
	
	static final void writeInt(final byte[] buffer, final int offset, final int value) {
		
		buffer[offset + 0] = (byte) (value >> 24);
		buffer[offset + 1] = (byte) (value >> 16);
		buffer[offset + 2] = (byte) (value >> 8);
		buffer[offset + 3] = (byte) value;
	}
	
	static final void writeLongAsLow6(final byte[] buffer, final int offset, final long value) {
		
		buffer[offset + 0] = (byte) (value >> 40);
		buffer[offset + 1] = (byte) (value >> 32);
		buffer[offset + 2] = (byte) (value >> 24);
		buffer[offset + 3] = (byte) (value >> 16);
		buffer[offset + 4] = (byte) (value >> 8);
		buffer[offset + 5] = (byte) value;
	}
	
	static final void writeLongReversedAsLow6(final byte[] buffer, final int offset, final long value) {
		
		buffer[offset + 5] = (byte) (value >> 40);
		buffer[offset + 4] = (byte) (value >> 32);
		buffer[offset + 3] = (byte) (value >> 24);
		buffer[offset + 2] = (byte) (value >> 16);
		buffer[offset + 1] = (byte) (value >> 8);
		buffer[offset + 0] = (byte) value;
	}
	
	static final void writeShort(final byte[] buffer, final int offset, final short value) {
		
		buffer[offset + 0] = (byte) (value >> 8);
		buffer[offset + 1] = (byte) value;
	}
	
	private final byte[] bufferBytes;
	
	/** vfs unique objects: luid -> guid + schedule ( + date ? )
	 *
	 * see cursorItem */
	Database dbItem;
	
	/** Luid resolver: guid -> luid
	 *
	 * see cursorItemGuid */
	Database dbItemGuid;
	
	/** Luid resolver: schedule -> luid
	 *
	 * see cursorItemGuid */
	Database dbItemQueue;
	
	Database dbTail;
	
	/** vfs: tree -> parentLuid;linkName
	 *
	 * see cursorTree */
	Database dbTree;
	
	Database dbTreeIndex;
	
	Database dbTreeUsage;
	
	private Environment environment;
	
	private final DatabaseEntry key;
	
	private final BdbjLocalS4 local;
	
	private final RowTree recordDropItemCleanTree = new RowTree();
	
	/** this.environment.beginTransaction( null, this.transactionConfig ); */
	private TransactionConfig transactionConfig;
	
	private final DatabaseEntry value;
	
	XctBdbjSimple xctGlobal;
	
	/**
	 *
	 */
	WorkerBdbj(final BdbjLocalS4 local) {
		
		this.bufferBytes = new byte[512 + 2048];
		assert this.bufferBytes.length >= S4Driver.TAIL_CAPACITY : "bufferBytes should be able to accomodate maximal tail";
		
		this.key = new DatabaseEntry();
		this.value = new DatabaseEntry();
		
		this.local = local;
	}
	
	void arsLinkDelete(//
			final XctBdbj xct, //
			final RecordBdbj record, //
			final Guid key, //
			final TreeLinkType mode, //
			final long modified//
	) throws Exception {
		
		this.arsLinkUpsert(xct, record, key, mode, modified, null);
	}
	
	void arsLinkUpdate(//
			final XctBdbj xct,
			final RecordBdbj record,
			final RecordBdbj newRecord,
			final Guid key,
			final Guid newKey,
			final TreeLinkType mode,
			final long modified,
			final Guid target//
	) throws Exception {
		
		this.arsLinkUpsert(xct, newRecord, newKey, mode, modified, target);
		this.arsLinkUpsert(xct, record, key, mode, modified, null);
	}
	
	void arsLinkUpsert(//
			final XctBdbj xct, //
			final RecordBdbj record,
			final Guid name,
			final TreeLinkType mode,
			final long modified,
			final Guid target//
	) throws Exception {
		
		assert record.luid != 0 : "Record's luid is zero!";
		
		assert target == null || target.isValid() : "Expected to be valid, target=" + target + ", type=" + target.getType() + ", inline=" + target.isInline();
		
		this.checkEnvironmentBug6();
		
		/** key;target;luid;key;mode;modified;target */
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		
		if (xct == null) {
			throw new IllegalArgumentException("Write activity is supposed to have an explicit transaction!");
		}
		
		final XctBdbj txn = xct;
		
		/** start build key */
		final int keyLength = Guid.writeGuid(name, this.bufferBytes, 0);
		final int guidLength = Guid.writeGuid(target, this.bufferBytes, keyLength);
		WorkerBdbj.writeLongAsLow6(this.bufferBytes, keyLength + guidLength, record.luid);
		System.arraycopy(this.bufferBytes, 0, this.bufferBytes, keyLength + guidLength + 6, keyLength);
		final int treeStart = keyLength + guidLength + 6 + keyLength;
		this.bufferBytes[treeStart] = (byte) mode.ordinal();
		final long modifiedValue = modified / 1000L;
		WorkerBdbj.writeLongAsLow6(this.bufferBytes, treeStart + 1, modifiedValue);
		System.arraycopy(this.bufferBytes, keyLength, this.bufferBytes, treeStart + 1 + 6, guidLength);
		/** end build key */
		final boolean completeVersion = this.dbTreeIndex != null;
		/** set 'tree' link */
		setTreeLink : {
			final Cursor cursorTree = txn.getCursorTree(this);
			/** keyX;targetX;(luid6;keyX);mode1;modified6;targetX */
			key.setData(this.bufferBytes, keyLength + guidLength, 6 + keyLength);
			/** exact search for 'tree' item */
			if (null == cursorTree.get(key, value, Get.SEARCH, WorkerBdbj.RO_FOR_WRITE)) {
				if (target == null) {
					/** already deleted - all done. **/
					return;
				}
				/** keyX;targetX;luid6;keyX;(mode1;modified6;targetX) */
				value.setData(this.bufferBytes, treeStart, 1 + 6 + guidLength);
				assert Guid.readGuid(this.bufferBytes, treeStart + 1 + 6).isValid() : "Expected to be valid!";
				cursorTree.put(key, value, Put.OVERWRITE, WorkerBdbj.WO_FOR_SAVE);
				
				if (!completeVersion) {
					/** simple tree record created - all done. **/
					return;
				}
				
				break setTreeLink;
			}
			/** item found, we know now that it is an existing one, we need to check for changes
			 * here */
			cleanupPrevious : {
				// check delete old indices
				final byte[] existingData = value.getData();
				final TreeLinkType previousMode = TreeLinkType.valueForIndex(existingData[0]);
				final long previousModified = WorkerBdbj.readLow6AsLong(existingData, 1) * 1000L;
				final Guid previousTarget = Guid.readGuid(existingData, 1 + 6);
				assert previousTarget.isValid() : "Expected to be valid!";
				/** modes and targets are all equals */
				/** record is not being deleted/unlinked **/
				if (previousMode == mode && target != null && previousTarget.equals(target)) {
					/** link date is also unchanged? */
					if (previousModified == modifiedValue) {
						/** nothing to change - all done. **/
						return;
					}
					/** update 'modified' only */
					value.setData(this.bufferBytes, treeStart, 1 + 6 + guidLength);
					assert Guid.readGuid(this.bufferBytes, treeStart + 1 + 6).isValid() : "Expected to be valid!";
					cursorTree.put(null, value, Put.CURRENT, WorkerBdbj.WO_FOR_SAVE);
					/** that's it! not going to touch anything else - all done. **/
					return;
				}
				
				if (!completeVersion) {
					break cleanupPrevious;
				}
				
				final boolean previousIndexing = previousMode.allowsSearchIndexing();
				final boolean previousUsage = previousMode.blocksGarbageCollection() && !previousTarget.isInline();
				final int freeStart = treeStart + 1 + 6 + guidLength;
				/** cleaning indices */
				if (previousIndexing) {
					/** (key;previousTarget;luid) */
					System.arraycopy(this.bufferBytes, 0, this.bufferBytes, freeStart, keyLength);
					final int prevLength = Guid.writeGuid(previousTarget, this.bufferBytes, freeStart + keyLength);
					System.arraycopy(this.bufferBytes, keyLength + guidLength, this.bufferBytes, freeStart + prevLength + keyLength, 6);
					value.setData(this.bufferBytes, freeStart, keyLength + prevLength + 6);
					
					final Cursor cursorTreeIndex = txn.getCursorTreeIndex(this);
					/** exact search **/
					if (null != cursorTreeIndex.get(value, null, Get.SEARCH, WorkerBdbj.RO_FOR_DROP)) {
						cursorTreeIndex.delete(WorkerBdbj.WO_FOR_DROP);
					}
				}
				/** cleaning usage */
				if (previousUsage) {
					
					/** (previousTargetX;luid6;keyX) */
					final int prevLength = Guid.writeGuid(previousTarget, this.bufferBytes, freeStart);
					System.arraycopy(this.bufferBytes, keyLength + guidLength, this.bufferBytes, freeStart + prevLength, 6);
					System.arraycopy(this.bufferBytes, 0, this.bufferBytes, freeStart + prevLength + 6, keyLength);
					value.setData(this.bufferBytes, freeStart, prevLength + 6 + keyLength);
					
					final Cursor cursorTreeUsage = txn.getCursorTreeUsage(this);
					/** exact search **/
					if (null != cursorTreeUsage.get(value, null, Get.SEARCH, WorkerBdbj.RO_FOR_DROP)) {
						cursorTreeUsage.delete(WorkerBdbj.WO_FOR_DROP);
					}
				}
				
				break cleanupPrevious;
			}
			
			/** record is being deleted **/
			if (target == null) {
				cursorTree.delete(WorkerBdbj.WO_FOR_DROP);
				/** everything is cleaned up - all done. **/
				return;
			}
			
			/** updating existing record **/
			value.setData(this.bufferBytes, treeStart, 1 + 6 + guidLength);
			assert Guid.readGuid(this.bufferBytes, treeStart + 1 + 6).isValid() : "Expected to be valid!";
			cursorTree.put(null, value, Put.CURRENT, WorkerBdbj.WO_FOR_SAVE);
			
			if (!completeVersion) {
				/** simple tree record updated - all done. **/
				return;
			}
			
			break setTreeLink;
		}
		/** populate secondary tables **/
		{
			/** update indices */
			if (mode.allowsSearchIndexing()) {
				/** (keyX;targetX;luid6);keyX;mode1;modified6;targetX */
				key.setData(this.bufferBytes, 0, keyLength + guidLength + 6);
				/** () */
				value.setData(this.bufferBytes, 0, 0);
				txn.getCursorTreeIndex(this).put(key, value, Put.OVERWRITE, WorkerBdbj.WO_FOR_SAVE);
			}
			/** update usage */
			if (mode.blocksGarbageCollection() && !target.isInline()) {
				/** keyX;(targetX;luid6;keyX);mode1;modified6;targetX */
				key.setData(this.bufferBytes, keyLength, guidLength + 6 + keyLength);
				/** () */
				value.setData(this.bufferBytes, 0, 0);
				txn.getCursorTreeUsage(this).put(key, value, Put.OVERWRITE, WorkerBdbj.WO_FOR_SAVE);
			}
		}
	}
	
	/** this method is ONLY called by storage garbage collection mechanism.
	 *
	 * there is NO need to clean `usage` table! */
	void arsRecordDelete(final XctBdbj xct, final RecordBdbj record) throws Exception {
		
		if (record.luid == 0) {
			/** not persistent anyway! */
			return;
		}
		
		if (xct == null) {
			throw new IllegalArgumentException("Write activity is supposed to have an explicit transaction!");
		}
		
		this.checkEnvironmentBug6();
		
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		
		final boolean completeVersion = this.dbTreeIndex != null;
		
		final RowItem recordItem = new RowItem();
		final RowItemGuid recordItemGuid = new RowItemGuid();
		
		final Guid guidInitial = record.guid;
		final long luidInitial = record.luid;
		/** common for any scaned items **/
		final int azimuth = guidInitial.hashCode();
		
		/** prepare data */
		final byte[] bufferBytes = this.bufferBytes;
		final int freeStart = 4 + 6 + 2 + Guid.MAX_LENGTH;
		
		/** TODO: think of 'tails' and 'roots' tables with strict key uniqueness! **/
		/** following block is only used if duplicates are scanned **/
		final long started = System.currentTimeMillis();
		boolean scanDuplicatesAsWell = Math.random() < 0.01 && !RowTree.guidIsTreeCollection(guidInitial);
		BasicQueue<RowItem> pendingDuplicateItemLuids = null;
		int duplicatesCount = 0;
		
		/**
		 *
		 */
		final Cursor cursorItem = xct.getCursorItem(this);
		final Cursor cursorItemGuid = xct.getCursorItemGuid(this);
		final Cursor cursorItemQueue = xct.getCursorItemQueue(this);
		
		// data ready
		dropItemRecords : for (RowItem nextPending = null;;) {
			final Guid guid = nextPending == null
				? guidInitial
				: nextPending.val1_guidX //
			;
			final long luid = nextPending == null
				? luidInitial
				: nextPending.key0_luid6//
			;
			// int guidSize;
			{
				final short scheduleBits = nextPending == null
					? record.scheduleBits
					: nextPending.val0_schedule2//
				;
				/** azimuth4;luid6;schedule2[;guidX] */
				WorkerBdbj.writeInt(bufferBytes, 0, azimuth);
				WorkerBdbj.writeLongAsLow6(bufferBytes, 4, luid);
				WorkerBdbj.writeShort(bufferBytes, 4 + 6, scheduleBits);
				// guidSize = Guid.writeGuid(guid, bufferBytes, 4 + 6 + 2);
			}
			
			assert azimuth == guid.hashCode() : "All azimuths expected to match here!";
			
			dropItemRecord : {
				/** azimuth4;(luid6);schedule2;guidX */
				key.setData(bufferBytes, 4, 6);
				/** find exact **/
				if (null == cursorItem.get(key, value, Get.SEARCH, WorkerBdbj.RO_FOR_DROP)) {
					/** nice, no record anyway... but strange, cause we have 'luid'? */
					break dropItemRecord;
				}
				
				/** check delete old indices, read schedule and stored guid **/
				RowItem.materializeRowItem(recordItem, key.getData(), value.getData());
				assert recordItem.val1_guidX.isValid() : "Expected to be valid!";
				assert recordItem.val1_guidX.equals(guid) : "Supposed to be equal!";
				
				/** delete existing guid **/
				dropItemGuidRecord : {
					
					scanDuplicatesAsWell : if (scanDuplicatesAsWell) {
						/** ...key guid: (azimuth:4);luid:6 */
						WorkerBdbj.writeInt(bufferBytes, freeStart, azimuth);
						key.setData(bufferBytes, freeStart, 4);
						if (null != cursorItemGuid.get(key, value, Get.SEARCH_GTE, WorkerBdbj.RO_FOR_DROP)) {
							WorkerBdbj.LOG.event(//
									"BDBJ-WORKER:DROP-REC:GUID-DUPS",
									"dropRecord random check for duplicates, azimuth: " + azimuth + ", guid: " + guid//
							);
							
							final long deadline = started + WorkerBdbj.DUPLICATE_TIME_LIMIT;
							/** kind of `boolean found = false` **/
							boolean foundExact = false;
							int duplicatesScanned = 0;
							Guid previousNonMatchingGuid = Guid.GUID_UNDEFINED;
							duplicatesScan : for (;;) {
								RowItemGuid.materializeRowItemGuid(recordItemGuid, key.getData(), value.getData());
								++duplicatesScanned;
								if (azimuth != recordItemGuid.key0_azimuth4) {
									/** kinda scanned all rows and it is not there, but definitely
									 * not there **/
									break duplicatesScan;
								}
								scanItemGuidRecords : {
									/** let's clean up SOME non-matching duplicate guids as well **/
									if (guidInitial.equals(recordItemGuid.val1_guidX)) {
										cursorItemGuid.delete(WorkerBdbj.WO_FOR_DROP);
										if (luidInitial == recordItemGuid.key1_luid6) {
											++duplicatesCount;
											foundExact = true;
											break scanItemGuidRecords;
										}
									} else//
									/** let's clean up SOME non-matching duplicate guids as well **/
									if (!recordItemGuid.val1_guidX.equals(previousNonMatchingGuid)) {
										previousNonMatchingGuid = recordItemGuid.val1_guidX;
										break scanItemGuidRecords;
									}
									
									++duplicatesCount;
									if (null == pendingDuplicateItemLuids) {
										pendingDuplicateItemLuids = new FifoQueueLinked<>();
									}
									final RowItem newRecordItem = new RowItem();
									newRecordItem.key0_luid6 = recordItemGuid.key1_luid6;
									newRecordItem.val0_schedule2 = recordItemGuid.val0_schedule2;
									newRecordItem.val1_guidX = recordItemGuid.val1_guidX;
									pendingDuplicateItemLuids.offerLast(newRecordItem);
									break scanItemGuidRecords;
								}
								if (null == cursorItemGuid.get(key, value, Get.NEXT, WorkerBdbj.RO_FOR_DROP)) {
									/** kinda scanned all rows and it is not there, but definitely
									 * not there **/
									foundExact = true;
									break duplicatesScan;
								}
								if (Engine.fastTime() > deadline) {
									break duplicatesScan;
								}
								if (duplicatesCount >= WorkerBdbj.DUPLICATE_COUNT_LIMIT) {
									break duplicatesScan;
								}
								if (duplicatesScanned >= WorkerBdbj.DUPLICATE_SCANNED_LIMIT) {
									break duplicatesScan;
								}
								continue duplicatesScan;
							}
							if (foundExact && null == pendingDuplicateItemLuids) {
								/** dropped exactly one record with matching luid **/
								break dropItemGuidRecord;
							}
							if (duplicatesCount > 1) {
								WorkerBdbj.LOG.event(//
										"BDBJ-WORKER:DROP-REC:GUID-DUPS",
										"dropRecord found " + duplicatesCount + " itemGuid duplicates while scanned " + duplicatesScanned + " rows, will drop, azimuth: " + azimuth//
								);
							}
							scanDuplicatesAsWell = false;
							if (!foundExact) {
								/** still need to delete exact record **/
								break scanDuplicatesAsWell;
							}
							break dropItemGuidRecord;
						}
						break dropItemGuidRecord;
					}
					
					/** ...key guid: (azimuth:4;luid:6) */
					WorkerBdbj.writeInt(bufferBytes, freeStart, azimuth);
					System.arraycopy(bufferBytes, 4, bufferBytes, freeStart + 4, 6);
					key.setData(bufferBytes, freeStart, 4 + 6);
					
					if (null != cursorItemGuid.get(key, null, Get.SEARCH, WorkerBdbj.RO_FOR_DROP)) {
						cursorItemGuid.delete(WorkerBdbj.WO_FOR_DROP);
					}
					break dropItemGuidRecord;
				}
				/** delete existing queue **/
				dropItemQueueRecord : {
					/** ... key queue: (prevSchedule:2;luid:6) */
					WorkerBdbj.writeShort(bufferBytes, freeStart, recordItem.val0_schedule2);
					System.arraycopy(bufferBytes, 4, bufferBytes, freeStart + 2, 6);
					key.setData(bufferBytes, freeStart, 2 + 6);
					
					if (null != cursorItemQueue.get(key, null, Get.SEARCH, WorkerBdbj.RO_FOR_DROP)) {
						cursorItemQueue.delete(WorkerBdbj.WO_FOR_DROP);
					}
					break dropItemQueueRecord;
				}
				
				dropAttachments : {
					if (RowTail.guidIsBinaryTail(guid)) {
						final Cursor cursorTail = xct.getCursorTail(this);
						/** azimuth4;(luid6);schedule2;guidX */
						key.setData(bufferBytes, 4, 6);
						if (null != cursorTail.get(key, null, Get.SEARCH, WorkerBdbj.RO_FOR_DROP)) {
							cursorTail.delete(WorkerBdbj.WO_FOR_DROP);
						}
						break dropAttachments;
					}
					if (RowTree.guidIsTreeCollection(guid)) {
						final Cursor cursorTree = xct.getCursorTree(this);
						Cursor cursorTreeUsage = null;
						Cursor cursorTreeIndex = null;
						final RowTree recordTree = this.recordDropItemCleanTree;
						/** azimuth4;(luid6);schedule2[;guidX] */
						key.setData(bufferBytes, 4, 6);
						if (null != cursorTree.get(key, value, Get.SEARCH_GTE, WorkerBdbj.RO_FOR_DROP)) {
							scanTree : for (;;) {
								{
									final byte[] keyData = key.getData();
									final long keyLuid = WorkerBdbj.readLow6AsLong(keyData, 0);
									if (keyLuid != luid) {
										break dropAttachments;
									}
									recordTree.key0_luid6 = keyLuid;
									recordTree.key1_keyX = Guid.readGuid(keyData, 6);
									// RowTree.materializeRowTree(recordTree, keyData,
									// value.getData());
									RowTree.materializeRowTreeValue(recordTree, value.getData());
								}
								{
									cursorTree.delete(WorkerBdbj.WO_FOR_DROP);
								}
								if (completeVersion) {
									if (!recordTree.val2_targetX.isInline() && TreeLinkType.valueForIndex(recordTree.val0_mode1).blocksGarbageCollection()) {
										if (cursorTreeUsage == null) {
											cursorTreeUsage = xct.getCursorTreeUsage(this);
										}
										RowTreeUsage.setupTreeUsageKey(key, bufferBytes, recordTree.val2_targetX, recordTree.key0_luid6, recordTree.key1_keyX);
										if (null != cursorTreeUsage.get(key, null, Get.SEARCH, WorkerBdbj.RO_FOR_DROP)) {
											cursorTreeUsage.delete(WorkerBdbj.WO_FOR_DROP);
										}
									}
									
									if (TreeLinkType.valueForIndex(recordTree.val0_mode1).allowsSearchIndexing()) {
										if (cursorTreeIndex == null) {
											cursorTreeIndex = xct.getCursorTreeIndex(this);
										}
										RowTreeIndex.setupTreeIndexKey(key, bufferBytes, recordTree.key1_keyX, recordTree.val2_targetX, recordTree.key0_luid6);
										if (null != cursorTreeIndex.get(key, null, Get.SEARCH, WorkerBdbj.RO_FOR_DROP)) {
											cursorTreeIndex.delete(WorkerBdbj.WO_FOR_DROP);
										}
									}
								}
								/**
								 *
								 */
								if (null == cursorTree.get(key, value, Get.NEXT, WorkerBdbj.RO_FOR_DROP)) {
									break scanTree;
								}
								continue scanTree;
							}
						}
						/** We are not cleaning it's children here */
						// System.out.println( new Error( ">>> delete container tree" )
						// );
						break dropAttachments;
					}
					break dropAttachments;
				}
				
				cursorItem.delete(WorkerBdbj.WO_FOR_DROP);
				break dropItemRecord;
			}
			if (null == pendingDuplicateItemLuids) {
				/** no duplicates ever **/
				return;
			}
			nextPending = pendingDuplicateItemLuids.pollFirst();
			if (null == nextPending) {
				/** out of duplicates **/
				WorkerBdbj.LOG.event(//
						"BDBJ-WORKER:DROP-REC:LUID-DUPS",
						"dropRecord dropped " + duplicatesCount + " itemGuid duplicates in items table, rows dropped, azimuth: " + azimuth + " in "
								+ Format.Compact.toPeriod(System.currentTimeMillis() - started)//
				);
				return;
			}
			continue dropItemRecords;
		}
	}
	
	void arsRecordUpsert(final XctBdbj xct, final RecordBdbj record) throws Exception {
		
		if (xct == null) {
			throw new IllegalArgumentException("Write activity is supposed to have an explicit transaction!");
		}
		
		this.checkEnvironmentBug6();
		
		final Guid guid = record.guid;
		
		final int azimuth = guid.hashCode();
		
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		
		/** azimuth4;luid6;schedule2;guidX */
		final byte[] bufferBytes = this.bufferBytes;
		
		/** prepare data: (azimuth4);luid6;schedule2;(guidX) */
		final int guidSize;
		{
			WorkerBdbj.writeInt(bufferBytes, 0, azimuth);
			guidSize = Guid.writeGuid(guid, bufferBytes, 4 + 6 + 2);
		}
		
		final Cursor cursorItemGuid = xct.getCursorItemGuid(this);
		
		boolean existingGuidFoundForNewRecord = false;
		
		/** try find existing record **/
		identifyLuid : if (record.luid == 0) {
			
			/** (azimuth:4);luid:6 */
			key.setData(bufferBytes, 0, 4);
			if (null != cursorItemGuid.get(key, value, Get.SEARCH_GTE, WorkerBdbj.RO_FOR_SCAN)) {
				final RowItemGuid recordItemGuid = new RowItemGuid();
				scanForGuid : for (;;) {
					RowItemGuid.materializeRowItemGuid(recordItemGuid, key.getData(), value.getData());
					if (azimuth != recordItemGuid.key0_azimuth4) {
						/** kinda scanned all rows and it is not there, but definitely not there **/
						break scanForGuid;
					}
					
					/** found matching guid **/
					if (record.guid.equals(recordItemGuid.val1_guidX)) {
						/** TODO: kinda scary to lock on an object used externally... **/
						synchronized (record) {
							if (record.luid != 0) {
								/** OOPS! Some other thread? **/
								return;
							}
							record.luid = recordItemGuid.key1_luid6;
							record.scheduleBits = recordItemGuid.val0_schedule2;
						}
						existingGuidFoundForNewRecord = true;
						break identifyLuid;
					}
					
					if (null == cursorItemGuid.get(key, value, Get.NEXT, WorkerBdbj.RO_FOR_SCAN)) {
						break scanForGuid;
					}
					continue scanForGuid;
				}
			}
			
			/** existing record was not found **/
			{
				final long luid = this.local.nextSequenceLuid();
				/** TODO: kinda scary to lock on an object used externally... **/
				synchronized (record) {
					if (record.luid != 0) {
						/** OOPS! Some other thread? **/
						return;
					}
					record.luid = luid;
				}
				break identifyLuid;
			}
		}
		
		/** prepare data: azimuth4;(luid6;schedule2);guidX */
		{
			WorkerBdbj.writeLongAsLow6(bufferBytes, 4, record.luid);
			WorkerBdbj.writeShort(bufferBytes, 4 + 6, record.scheduleBits);
		}
		
		if (!existingGuidFoundForNewRecord) {
			/**
			 *
			 */
			final Cursor cursorItem = xct.getCursorItem(this);
			final Cursor cursorItemQueue = xct.getCursorItemQueue(this);
			
			setRecord : {
				setItemRecord : {
					/** azimuth4;(luid6);schedule2;guidX **/
					key.setData(bufferBytes, 4, 6);
					/** exact search **/
					if (null == cursorItem.get(key, value, Get.SEARCH, WorkerBdbj.RO_FOR_WRITE)) {
						/** azimuth:4;luid:6;(schedule:2;guid:X) **/
						value.setData(bufferBytes, 4 + 6, 2 + guidSize);
						cursorItem.put(key, value, Put.OVERWRITE, WorkerBdbj.WO_FOR_SAVE);
						break setItemRecord;
					}
					// check delete old indices
					final byte[] valueBytes = value.getData();
					final short previousScheduleBits = (short) (((valueBytes[0] & 0xFF) << 8) + (valueBytes[1] & 0xFF));
					final Guid previousGuid = Guid.readGuid(valueBytes, 2);
					assert previousGuid.isValid() : "Expected to be valid!";
					if (previousScheduleBits == record.scheduleBits && previousGuid.equals(record.guid)) {
						/** nothing to change, templates (like 'tail') must already be committed for
						 * an existing record. */
						break setRecord;
					}
					// delete old indices
					final int freeStart = 4 + 6 + 2 + guidSize;
					/** is it really possible? */
					if (!previousGuid.equals(guid)) {
						WorkerBdbj.writeInt(bufferBytes, freeStart, previousGuid.hashCode());
						System.arraycopy(bufferBytes, 4, bufferBytes, freeStart + 4, 6);
						/** (prevAzimuth:4;luid:6) */
						value.setData(bufferBytes, freeStart, 4 + 6);
						if (null != cursorItemGuid.get(value, null, Get.SEARCH, WorkerBdbj.RO_FOR_DROP)) {
							cursorItemGuid.delete(WorkerBdbj.WO_FOR_DROP);
						}
					}
					/** that's more likely */
					if (previousScheduleBits != record.scheduleBits) {
						WorkerBdbj.writeShort(bufferBytes, freeStart, previousScheduleBits);
						System.arraycopy(bufferBytes, 4, bufferBytes, freeStart + 2, 6);
						/** queue: prevSchedule:2;luid:6 */
						value.setData(bufferBytes, freeStart, 2 + 6);
						if (null != cursorItemQueue.get(value, null, Get.SEARCH, WorkerBdbj.RO_FOR_DROP)) {
							cursorItemQueue.delete(WorkerBdbj.WO_FOR_DROP);
						}
					}
					/** azimuth:4;luid:6;(schedule:2;guid:X) */
					value.setData(bufferBytes, 4 + 6, 2 + guidSize);
					// assert Guid.readGuid(bufferBytes, 4 + 6 + 2).isValid() : "Expected to be
					// valid!";
					cursorItem.put(null, value, Put.CURRENT, WorkerBdbj.WO_FOR_SAVE);
					break setItemRecord;
				}
				/* setItemGuidRecord: */ {
					/** (azimuth:4;luid:6);schedule:2;guid */
					key.setData(bufferBytes, 0, 4 + 6);
					/** azimuth:4;luid:6;(schedule:2;guid) */
					value.setData(bufferBytes, 4 + 6, 2 + guidSize);
					cursorItemGuid.put(key, value, Put.OVERWRITE, WorkerBdbj.WO_FOR_SAVE);
				}
				/* setItemQueueRecord: */ {
					/** azimuth:4;luid:6;schedule:2;guid
					 *
					 * ->
					 *
					 * pad:2;schedule:2;luid:6;schedule:2;guid */
					WorkerBdbj.writeShort(bufferBytes, 2, record.scheduleBits);
					/** pad:2;(schedule:2;luid:6);schedule:2;guid */
					key.setData(bufferBytes, 2, 2 + 6);
					/** () */
					value.setData(bufferBytes, 0, 0);
					cursorItemQueue.put(key, value, Put.OVERWRITE, WorkerBdbj.WO_FOR_SAVE);
				}
			}
		}
		recordsAttachement : {
			if (RowTail.guidIsBinaryTail(guid)) {
				final TransferCopier tail = record instanceof final RecTemplate recTemplate
					? (TransferCopier) recTemplate.getAttachment()
					: null;
				if (tail != null) {
					final Cursor cursorTail = xct.getCursorTail(this);
					/** pad2;schedule2;luid6;schedule2;guidX -> pad2;schedule2;luid6;dataX */
					final int tailSize = tail.copy(0, bufferBytes, 4 + 6, S4Driver.TAIL_CAPACITY);
					/** pad2;schedule2;(luid6);schedule2;guidX */
					key.setData(bufferBytes, 4, 6);
					
					if (null == cursorTail.get(key, value, Get.SEARCH, WorkerBdbj.RO_FOR_WRITE)) {
						/** pad2;schedule2;luid6;(dataX) */
						value.setData(bufferBytes, 4 + 6, tailSize);
						cursorTail.put(key, value, Put.OVERWRITE, WorkerBdbj.WO_FOR_SAVE);
						break recordsAttachement;
					}
					
					final byte[] valueBytes = value.getData();
					compare : {
						if (valueBytes.length != tailSize) {
							/** update value **/
							break compare;
						}
						for (int i = tailSize - 1; i >= 0; --i) {
							if (valueBytes[i] != bufferBytes[4 + 6 + i]) {
								/** update value **/
								break compare;
							}
						}
						/** no need to update value -- same bytes **/
						break recordsAttachement;
					}
					/** pad2;schedule2;luid6;(dataX) */
					value.setData(bufferBytes, 4 + 6, tailSize);
					cursorTail.put(null, value, Put.CURRENT, WorkerBdbj.WO_FOR_SAVE);
					break recordsAttachement;
				}
				break recordsAttachement;
			}
			if (RowTree.guidIsTreeCollection(guid)) {
				@SuppressWarnings("unchecked")
				final Collection<ReferenceBdbj> tree = record instanceof final RecTemplate recTemplate
					? (Collection<ReferenceBdbj>) recTemplate.getAttachment()
					: null;
				if (tree != null) {
					for (final ReferenceBdbj impl : tree) {
						this.arsLinkUpsert(xct, record, impl.key, impl.mode, impl.modified, impl.value);
					}
				}
				break recordsAttachement;
			}
		}
	}
	
	private void checkEnvironmentBug6() {
		
		final Environment environment = this.environment;
		if (environment != null && !environment.isValid()) {
			WorkerBdbj.LOG.event(//
					"BDBJ-WORKER:FAILURE:FATAL",
					"Environment is invalid!",
					Format.Throwable.toText(new IllegalStateException("this:" + this + ", env:" + environment))//
			);
			try {
				this.stop();
			} catch (final Throwable t) {
				//
			}
			try {
				environment.close();
			} catch (final Throwable t) {
				//
			}
			Runtime.getRuntime().exit(-37);
		}
	}
	@Override
	public void commitHighLevel() throws Exception {
		
		this.environment.sync();
		// ignore, low-level faster on BDBJE
	}
	@Override
	public void commitLowLevel() throws Exception {
		
		// ignore, low-level faster on BDBJE
		this.checkEnvironmentBug6();
	}
	
	@Override
	public XctBdbj createGlobalCommonTransaction() {
		
		if (this.transactionConfig == null) {
			return this.xctGlobal;
		}
		this.checkEnvironmentBug6();
		this.xctGlobal.reset();
		return this.xctGlobal;
	}
	
	@Override
	public XctBdbj createNewWorkerTransaction() {
		
		if (this.transactionConfig == null) {
			return this.xctGlobal;
		}
		this.checkEnvironmentBug6();
		this.xctGlobal.reset();
		return new XctBdbjSimple(this, this.environment.beginTransaction(null, this.transactionConfig), CursorConfig.READ_COMMITTED);
	}
	
	@Override
	public RecordBdbj createRecordTemplate() {
		
		return new RecordBdbjTemplate();
	}
	
	@Override
	public ReferenceBdbj createReferenceTemplate() {
		
		return new ReferenceBdbj();
	}
	
	/** @param txn
	 * @param e
	 * @throws RuntimeException */
	private Exception handleTransactionTimeoutException(//
			final XctBdbj txn,
			final TransactionTimeoutException e//
	) {
		
		final String text;
		if (txn == this.xctGlobal) {
			System.out.println(">>>>>> " + (text = "unexpected global transaction problem: txn=" + txn));
			this.xctGlobal = this.transactionConfig != null
				? new XctBdbjTxnTempGlobal(this, this.environment)
				: new XctBdbjNoTxnGlobal(this);
			txn.destroy();
		} else {
			txn.destroy();
			System.out.println(">>>>>> " + (text = "unexpected task transaction problem: txn=" + txn));
		}
		return new RuntimeException(text, e);
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
	
	/** @return */
	public List<String> internStorageTables() {
		
		return this.environment.getDatabaseNames();
	}
	
	@Override
	public int readCheckReferenced(//
			final BasicQueue<RecordBdbj> pendingRecords,
			final BasicQueue<RecordBdbj> targetReferenced,
			final BasicQueue<RecordBdbj> targetUnreferenced//
	) throws Exception {
		
		this.checkEnvironmentBug6();
		
		final XctBdbjSimple txn = this.xctGlobal;
		
		try {
			final byte[] bufferBytes = this.bufferBytes;
			final DatabaseEntry key = this.key;
			int count = 0;
			
			/** sort records (probably already sorted) */
			if (!Engine.MODE_SIZE && pendingRecords.hasNext()) {
				final SortedSet<RecordBdbj> set = new TreeSet<>(RecImpl.COMPARATOR_RECORD_GUID);
				for (RecordBdbj record; (record = pendingRecords.pollFirst()) != null;) {
					set.add(record);
				}
				for (final RecordBdbj record : set) {
					pendingRecords.offerLast(record);
				}
			}
			
			try (final Cursor cursorTreeUsage = this.dbTreeUsage.openCursor(txn.txn, CursorConfig.READ_COMMITTED)) {
				records : for (RecordBdbj record; (record = pendingRecords.pollFirst()) != null;) {
					final Guid guid = record.guid;
					
					if (guid == Guid.GUID_NULL) {
						WorkerBdbj.LOG.event(//
								"BDBJ-WORKER:FAILURE:SUPPRESSED",
								"readCheckReferenced is not supposed to get GUID_NULL record, ",
								Format.Throwable.toText(new IllegalStateException("this:" + this + ", env:" + this.environment))//
						);
						targetReferenced.offerLast(record);
						continue records;
					}
					
					++count;
					
					/** (previousTargetX);luid6;keyX */
					final int guidSize = Guid.writeGuid(guid, bufferBytes, 0);
					key.setData(bufferBytes, 0, guidSize);
					
					/** scan first of multiple **/
					if (null == cursorTreeUsage.get(key, null, Get.SEARCH_GTE, WorkerBdbj.RO_FOR_SCAN)) {
						/** not referenced **/
						targetUnreferenced.offerLast(record);
						continue records;
					}
					
					/** referenced or not based on found key match **/
					final byte[] bytes = key.getData();
					(Guid.readEquals(bytes, 0, guid)
						? targetReferenced
						: targetUnreferenced).offerLast(record);
					
					/** check next **/
					continue records;
				}
			}
			return count;
		} catch (final TransactionTimeoutException e) {
			throw this.handleTransactionTimeoutException(txn, e);
		}
	}
	
	int readContainerContentsRange(//
			final XctBdbj xct,
			final Function<ReferenceBdbj, ?> target,
			final RecordBdbj record,
			final Guid keyStart,
			final Guid keyStop,
			final int limit,
			final boolean backwards//
	) throws Exception {
		
		assert record.luid != 0 : "Record's luid is zero!";
		
		this.checkEnvironmentBug6();
		
		final XctBdbj txn = xct == null
			? this.xctGlobal
			: xct;
		
		assert record != null : "Record is null";
		final long luid = record.luid;
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		if (keyStart == null) {
			WorkerBdbj.writeLongAsLow6(
					this.bufferBytes, //
					0,
					/** when doing 'backwards' with no starting key we should start with next
					 * luid */
					backwards
						? luid + 1
						: luid);
			/** (luid6) */
			key.setData(this.bufferBytes, 0, 6);
		} else {
			WorkerBdbj.writeLongAsLow6(this.bufferBytes, 0, luid);
			final int keyLength = Guid.writeGuid(keyStart, this.bufferBytes, 6);
			/** (luid6,ketStartX) */
			key.setData(this.bufferBytes, 0, 6 + keyLength);
		}
		
		try {
			final Cursor cursorTree = txn.getCursorTree(this);
			{
				OperationResult status = cursorTree.get(
						key,
						value,
						Get.SEARCH_GTE,
						xct == null
							? WorkerBdbj.RO_FOR_READ
							: null);
				prepareBackwards : if (backwards) {
					if (null == status) {
						/** The record is LAST or table is empty! Rare case 8-) */
						status = cursorTree.get(
								key,
								value,
								Get.LAST,
								xct == null
									? WorkerBdbj.RO_FOR_READ
									: null);
						break prepareBackwards;
					}
					/** now the cursor is either on the record or on the next one */
					final byte[] keyData = key.getData();
					/** exact key match found */
					if (WorkerBdbj.readLow6AsLong(keyData, 0) == luid && //
							(keyStart == null || Guid.readCompare(keyData, 6, keyStart) == 0)//
					) {
						break prepareBackwards;
					}
					/** move to backwards once */
					status = cursorTree.get(
							key,
							value,
							Get.PREV,
							xct == null
								? WorkerBdbj.RO_FOR_READ
								: null);
					break prepareBackwards;
				}
				for (int left = limit;;) {
					if (null == status) {
						return limit - left;
					}
					/** key tree: (luid6,keyX) */
					final byte[] keyData = key.getData();
					if (WorkerBdbj.readLow6AsLong(keyData, 0) != luid) {
						return limit - left;
					}
					if (keyStop != null && (backwards
						? Guid.readCompare(keyData, 6, keyStop) <= 0
						: Guid.readCompare(keyData, 6, keyStop) >= 0)) {
						return limit - left;
					}
					{
						/** value tree: mode1;modified6;targetX */
						final byte[] bytes = value.getData();
						final ReferenceBdbj reference = new ReferenceBdbj();
						reference.key = Guid.readGuid(keyData, 6);
						reference.mode = TreeLinkType.valueForIndex(bytes[0]);
						reference.modified = WorkerBdbj.readLow6AsLong(bytes, 1) * 1000L;
						reference.value = Guid.readGuid(bytes, 1 + 6);
						assert reference.value.isValid() : "Stored link target expected to be valid!";
						target.apply(reference);
					}
					/** check update limit **/
					if (--left == 0) {
						/** limit reached **/
						return limit;
					}
					/** next record **/
					status = cursorTree.get( //
							key,
							value,
							backwards
								? Get.PREV
								: Get.NEXT,
							xct == null
								? WorkerBdbj.RO_FOR_READ
								: null//
					);
				}
			}
		} catch (final TransactionTimeoutException e) {
			throw this.handleTransactionTimeoutException(txn, e);
		}
	}
	
	ReferenceBdbj readContainerElement(//
			final XctBdbj xct,
			final RecordBdbj record,
			final Guid name//
	) throws Exception {
		
		assert record.luid != 0 : "Record's luid is zero!";
		
		this.checkEnvironmentBug6();
		
		final XctBdbj txn = xct == null
			? this.xctGlobal
			: xct;
		
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		
		/** key tree: (luid6,keyX) */
		WorkerBdbj.writeLongAsLow6(this.bufferBytes, 0, record.luid);
		final int keyLength = Guid.writeGuid(name, this.bufferBytes, 6);
		key.setData(this.bufferBytes, 0, 6 + keyLength);
		
		try {
			
			/** search exact **/
			if (null == txn.getCursorTree(this).get(
					key,
					value,
					Get.SEARCH,
					xct == null
						? WorkerBdbj.RO_FOR_READ
						: null)) {
				/** not found **/
				return null;
			}
			
			/** value tree: mode1;modified6;targetX */
			final byte[] bytes = value.getData();
			final ReferenceBdbj result = new ReferenceBdbj();
			result.key = name;
			result.mode = TreeLinkType.valueForIndex(bytes[0]);
			result.modified = WorkerBdbj.readLow6AsLong(bytes, 1) * 1000L;
			result.value = Guid.readGuid(bytes, 1 + 6);
			assert result.value.isValid() : "Stored link target expected to be valid, name: " + name + " (" + name.getInlineValue() + "), target:" + result.value;
			return result;
			
		} catch (final TransactionTimeoutException e) {
			throw this.handleTransactionTimeoutException(txn, e);
		}
	}
	
	RecordBdbj readRecord(//
			final XctBdbj xct,
			final Guid guid//
	) throws Exception {
		
		this.checkEnvironmentBug6();
		
		final XctBdbj txn = xct == null
			? this.xctGlobal
			: xct;
		try {
			
			final DatabaseEntry key = this.key;
			final DatabaseEntry value = this.value;
			
			final Cursor cursorItemGuid = txn.getCursorItemGuid(this);
			
			/** azimuth:4 */
			WorkerBdbj.writeInt(this.bufferBytes, 0, guid.hashCode());
			key.setData(this.bufferBytes, 0, 4);
			
			for (OperationResult status = cursorItemGuid.get(
					key,
					value,
					Get.SEARCH_GTE,
					xct == null
						? WorkerBdbj.RO_FOR_READ
						: null);;) {
				if (null == status) {
					/** not found **/
					return null;
				}
				
				final byte[] bytesKey = key.getData();
				assert bytesKey.length == 4 + 6 : "key in itemGuid table should be exactly 4+6 bytes in size";
				
				/** compare azimuth **/
				if (!WorkerBdbj.compareBytes(this.bufferBytes, 0, bytesKey, 0, 4)) {
					/** not found **/
					return null;
				}
				
				/** value guid: schedule2;guidX */
				final byte[] bytes = value.getData();
				if (Guid.readEquals(bytes, 2, guid)) {
					final RecordBdbj result = new RecordBdbj();
					result.guid = guid;
					/** azimuth4;(luid6);schedule2;guidX */
					result.luid = WorkerBdbj.readLow6AsLong(bytesKey, 4);
					/** azimuth4;luid6;(schedule2);guidX */
					result.scheduleBits = (short) (((bytes[0] & 0xFF) << 8) + (bytes[1] & 0xFF));
					return result;
				}
				
				/** azimuth is not unique, search next matching record **/
				status = cursorItemGuid.get(
						key,
						value,
						Get.NEXT,
						xct == null
							? WorkerBdbj.RO_FOR_READ
							: null);
			}
		} catch (final TransactionTimeoutException e) {
			throw this.handleTransactionTimeoutException(txn, e);
		}
	}
	
	byte[] readRecordTail(//
			final XctBdbj xct,
			final RecordBdbj record//
	) throws Exception {
		
		assert record.luid != 0 : "Record's luid is zero!";
		
		this.checkEnvironmentBug6();
		
		final XctBdbj txn = xct == null
			? this.xctGlobal
			: xct;
		try {
			
			final DatabaseEntry key = this.key;
			final DatabaseEntry value = this.value;
			
			final Cursor cursorTail = txn.getCursorTail(this);
			
			/** (luid6) */
			WorkerBdbj.writeLongAsLow6(this.bufferBytes, 0, record.luid);
			key.setData(this.bufferBytes, 0, 6);
			
			/** search exact match **/
			if (null == cursorTail.get(
					key,
					value,
					Get.SEARCH,
					xct == null
						? WorkerBdbj.RO_FOR_READ
						: null)) {
				/** not found **/
				return null;
			}
			/** no 'clone' needed: For a DatabaseEntry that is used as an output parameter, the byte
			 * array will always be a newly allocated array. */
			return value.getData();
		} catch (final TransactionTimeoutException e) {
			throw this.handleTransactionTimeoutException(txn, e);
		}
	}
	
	@Override
	public void reset() {
		
		if (this.xctGlobal != null) {
			this.xctGlobal.reset();
		}
	}
	
	int searchBetween(//
			final XctBdbj xct, //
			final Function<Long, ?> target, //
			final Guid name, //
			final Guid value1, //
			final Guid value2, //
			final int limit //
	) throws Exception {
		
		final boolean completeVersion = this.dbTreeIndex != null;
		if (!completeVersion) {
			throw new UnsupportedOperationException("Given instance doesn't support this method due to lack of indices!");
		}
		
		this.checkEnvironmentBug6();
		
		final int keyLength = Guid.writeGuid(name, this.bufferBytes, 0);
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		{
			final int guidLength = Guid.writeGuid(value1, this.bufferBytes, keyLength);
			key.setData(this.bufferBytes, 0, keyLength + guidLength);
		}
		
		final XctBdbj txn = xct == null
			? this.xctGlobal
			: xct;
		
		final Cursor cursorTreeIndex = txn.getCursorTreeIndex(this);
		for (int left = limit;;) {
			OperationResult status = cursorTreeIndex.get(
					key,
					value,
					Get.SEARCH_GTE,
					xct == null
						? WorkerBdbj.RO_FOR_READ
						: null);
			if (null == status) {
				return limit - left;
			}
			final byte[] keyData = key.getData();
			if (!WorkerBdbj.compareBytes(keyData, 0, this.bufferBytes, 0, keyLength) || Guid.readCompare(keyData, keyLength, value2) > 0) {
				return limit - left;
			}
			final long luid = WorkerBdbj.readLow6AsLong(keyData, keyLength + Guid.readGuidByteCount(keyData, keyLength));
			target.apply(Long.valueOf(luid));
			if (--left == 0) {
				return limit;
			}
			status = cursorTreeIndex.get(
					key,
					value,
					Get.NEXT,
					xct == null
						? WorkerBdbj.RO_FOR_READ
						: null);
		}
	}
	
	int searchEquals(//
			final XctBdbj xct, //
			final Collection<Long> target, //
			final Guid name, //
			final Guid value1, //
			final int limit//
	) throws Exception {
		
		final boolean completeVersion = this.dbTreeIndex != null;
		if (!completeVersion) {
			throw new UnsupportedOperationException("Given instance doesn't support this method due to lack of indices!");
		}
		
		this.checkEnvironmentBug6();
		
		final int keyLength = Guid.writeGuid(name, this.bufferBytes, 0);
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		final int guidLength = Guid.writeGuid(value1, this.bufferBytes, keyLength);
		/** (key;target)[;luid?] */
		key.setData(this.bufferBytes, 0, keyLength + guidLength);
		
		final XctBdbj txn = xct == null
			? this.xctGlobal
			: xct;
		
		final Cursor cursorTreeIndex = txn.getCursorTreeIndex(this);
		for (int left = limit;;) {
			OperationResult status = cursorTreeIndex.get(
					key,
					value,
					Get.SEARCH_GTE,
					xct == null
						? WorkerBdbj.RO_FOR_READ
						: null);
			if (null == status) {
				return limit - left;
			}
			final byte[] bytes = key.getData();
			if (!WorkerBdbj.compareBytes(bytes, 0, this.bufferBytes, 0, keyLength + guidLength)) {
				return limit - left;
			}
			final long luid = WorkerBdbj.readLow6AsLong(bytes, keyLength + guidLength);
			target.add(Long.valueOf(luid));
			if (--left == 0) {
				return limit;
			}
			status = cursorTreeIndex.get(
					key,
					value,
					Get.NEXT,
					xct == null
						? WorkerBdbj.RO_FOR_READ
						: null);
		}
	}
	
	@Override
	public void searchScheduled(//
			final short scheduleBits, //
			final int limit, //
			final BasicQueue<RecordBdbj> targetScheduled//
	) throws Exception {
		
		this.checkEnvironmentBug6();
		
		final byte[] bufferBytes = new byte[2 + 6];
		WorkerBdbj.writeShort(bufferBytes, 0, scheduleBits);
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		key.setData(bufferBytes, 0, 2);
		
		/** TODO: Not really good condition */
		if (Engine.MODE_SIZE) {
			
			try (XctBdbj xct = this.createNewWorkerTransaction()) {
				boolean changes = false;
				selectSchedule : try (final Cursor cursorItemQueue = xct.getCursorItemQueue(this)) {
					try (final Cursor cursorItem = xct.getCursorItem(this)) {
						OperationResult status = cursorItemQueue.get(key, null, Get.SEARCH_GTE, WorkerBdbj.RO_FOR_SCAN);
						nextLuid : for (int left = limit;;) {
							if (null == status) {
								break selectSchedule;
							}
							final byte[] keyData = key.getData();
							if (!WorkerBdbj.compareBytes(keyData, 0, bufferBytes, 0, 2)) {
								break selectSchedule;
							}
							final RecordBdbj record = new RecordBdbj();
							record.luid = WorkerBdbj.readLow6AsLong(keyData, 2);
							
							key.setData(keyData, 2, 6);
							if (null == cursorItem.get(key, value, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN)) {
								WorkerBdbj.LOG.event(//
										"BDBJ-WORKER:INVALID:SCHEDULE",
										"searchScheduled invalid records detected and will be dropped, luid: " + record.luid//
								);
								cursorItemQueue.delete(WorkerBdbj.WO_FOR_DROP);
								changes = true;
							} else //
							{
								record.guid = Guid.readGuid(value.getData(), 2);
								record.scheduleBits = scheduleBits;
								targetScheduled.offerLast(record);
							}
							if (--left == 0) {
								break selectSchedule;
							}
							status = cursorItemQueue.get(key, null, Get.NEXT, WorkerBdbj.RO_FOR_SCAN);
							continue nextLuid;
						}
					}
				}
				if (changes) {
					xct.commit();
				}
			}
			return;
		}
		{
			final BasicQueue<RecordBdbj> recordsLuids = new FifoQueueLinked<>();
			final BasicQueue<RecordBdbj> recordsInvalid = new FifoQueueLinked<>();
			
			try (XctBdbj xct = this.createGlobalCommonTransaction()) {
				collectLuids : try (final Cursor cursorItemQueue = xct.getCursorItemQueue(this)) {
					OperationResult status = cursorItemQueue.get(key, null, Get.SEARCH_GTE, WorkerBdbj.RO_FOR_SCAN);
					nextLuid : for (int left = limit;;) {
						if (null == status) {
							break collectLuids;
						}
						final byte[] keyData = key.getData();
						if (!WorkerBdbj.compareBytes(keyData, 0, bufferBytes, 0, 2)) {
							break collectLuids;
						}
						final RecordBdbj record = new RecordBdbj();
						record.luid = WorkerBdbj.readLow6AsLong(keyData, 2);
						record.scheduleBits = scheduleBits;
						/** TODO: add in-memory cache check here */
						//
						recordsLuids.offerLast(record);
						if (--left == 0) {
							break collectLuids;
						}
						status = cursorItemQueue.get(key, null, Get.NEXT, WorkerBdbj.RO_FOR_SCAN);
						continue nextLuid;
					}
				}
				
				if (!recordsLuids.hasNext()) {
					return;
				}
				
				collectGuids : try (final Cursor cursorItem = xct.getCursorItem(this)) {
					nextLuid : for (;;) {
						final RecordBdbj record = recordsLuids.pollFirst();
						if (record == null) {
							break collectGuids;
						}
						if (record.guid != null) {
							/** TODO: should be set by in-memory cache check */
							targetScheduled.offerLast(record);
							continue nextLuid;
						}
						WorkerBdbj.writeLongAsLow6(bufferBytes, 0, record.luid);
						key.setData(bufferBytes, 0, 6);
						if (null == cursorItem.get(key, value, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN)) {
							recordsInvalid.offerLast(record);
							continue nextLuid;
						}
						record.guid = Guid.readGuid(value.getData(), 2);
						targetScheduled.offerLast(record);
						continue nextLuid;
					}
				}
			}
			
			if (!recordsInvalid.hasNext()) {
				return;
			}
			try (XctBdbj xct = this.createNewWorkerTransaction()) {
				deleteInvalid : try (final Cursor cursorItemQueue = xct.getCursorItemQueue(this)) {
					nextRecord : for (;;) {
						final RecordBdbj record = recordsInvalid.pollFirst();
						if (record == null) {
							break deleteInvalid;
						}
						WorkerBdbj.writeShort(bufferBytes, 0, record.scheduleBits);
						WorkerBdbj.writeLongAsLow6(bufferBytes, 2, record.luid);
						key.setData(bufferBytes, 0, 2 + 6);
						if (null != cursorItemQueue.get(key, null, Get.SEARCH, WorkerBdbj.RO_FOR_DROP)) {
							WorkerBdbj.LOG.event(//
									"BDBJ-WORKER:INVALID:SCHEDULE",
									"searchScheduled invalid records detected and will be dropped, luid: " + record.luid//
							);
							cursorItemQueue.delete(WorkerBdbj.WO_FOR_DROP);
						}
						continue nextRecord;
					}
				}
				xct.commit();
			}
		}
	}
	
	@Override
	public void start() throws Exception {
		
		this.environment = this.local.environment;
		
		assert this.environment != null : "Environment is NULL!";
		
		this.dbItem = this.local.dbItem;
		this.dbItemGuid = this.local.dbItemGuid;
		this.dbItemQueue = this.local.dbItemQueue;
		this.dbTail = this.local.dbTail;
		this.dbTree = this.local.dbTree;
		this.dbTreeIndex = this.local.dbTreeIndex;
		this.dbTreeUsage = this.local.dbTreeUsage;
		
		{
			/** TODO: (and there is another place) */
			final boolean transactional = !this.local.instanceType.allowTruncate() || true;
			if (transactional) {
				final TransactionConfig config = new TransactionConfig();
				config.setReadCommitted(true);
				config.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
				this.transactionConfig = config;
			} else {
				this.transactionConfig = null;
			}
		}
		this.xctGlobal = this.transactionConfig != null
			? new XctBdbjTxnTempGlobal(this, this.environment)
			: new XctBdbjNoTxnGlobal(this);
		
		this.checkEnvironmentBug6();
	}
	
	@Override
	public void stop() throws Exception {
		
		if (this.environment == null) {
			return;
		}
		
		if (this.xctGlobal != null) {
			try {
				this.xctGlobal.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.xctGlobal = null;
		}
		
		this.dbItem = null;
		this.dbItemGuid = null;
		this.dbItemQueue = null;
		this.dbTail = null;
		this.dbTree = null;
		this.dbTreeIndex = null;
		this.dbTreeUsage = null;
		this.environment = null;
	}
	
	@Override
	public long storageCalculate() throws Exception {
		
		return this.local.storageCalculate();
	}
	
	@Override
	public void storageTruncate() throws Exception {
		
		this.local.storageTruncate();
	}
	
	@Override
	public String toString() {
		
		return this.getClass().getSimpleName() + "(" + this.local + ")";
	}
}
