package ru.myx.ae3.vfs.s4.lcl.bdbj_compare;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentStats;
import com.sleepycat.je.ExceptionEvent;
import com.sleepycat.je.ExceptionListener;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

import ru.myx.ae3.Engine;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.e4.act.Manager.Factory.TYPE;
import ru.myx.ae3.e4.act.ProcessExecutionType;
import ru.myx.ae3.help.Create;
import ru.myx.ae3.help.Format;
import ru.myx.ae3.know.Guid;
import ru.myx.ae3.reflect.ReflectionEnumerable;
import ru.myx.ae3.reflect.ReflectionExplicit;
import ru.myx.ae3.report.Report;
import ru.myx.ae3.report.ReportReceiver;
import ru.myx.ae3.vfs.TreeLinkType;
import ru.myx.ae3.vfs.s4.common.RecTemplate;
import ru.myx.ae3.vfs.s4.common.RepairChecker;
import ru.myx.ae3.vfs.s4.common.S4StoreType;
import ru.myx.ae3.vfs.s4.driver.S4Driver;
import ru.myx.ae3.vfs.s4.driver.S4WorkerInterface;
import ru.myx.ae3.vfs.s4.driver.S4WorkerTransaction;
import ru.myx.ae3.vfs.s4.driver.S4WorkerTransactionDummy;
import ru.myx.ae3.vfs.s4.impl.S4TreeDriver;
import ru.myx.ae3.vfs.s4.impl.S4TreeWorker;
import ru.myx.bdbje.upgrade.DbPreUpgrade_4_1;
import ru.myx.util.BasicQueue;

/** NO TXN, Single Threaded
 *
 * @author myx */
@ProcessExecutionType(type = TYPE.SINGLE_THREAD)
public class BdbjCompare
		implements
			S4TreeDriver<RecordBdbj, ReferenceBdbj, Long>,
			S4TreeWorker<RecordBdbj, ReferenceBdbj, Long>,
			S4WorkerInterface<RecordBdbj, ReferenceBdbj, Long> {

	private static final String BASE_NAME = "bdb2-";

	private static final WeakHashMap<String, BdbjCompare> INSTANCES = new WeakHashMap<>();

	/** Logger */
	static final ReportReceiver LOG = Report.createReceiver("ae3.s4");

	private static final boolean compareBytes(final byte[] bytes1, final int offset1, final byte[] bytes2, final int offset2, final int length) {

		for (int i = 0; i < length; ++i) {
			if (bytes1[offset1 + i] != bytes2[offset2 + i]) {
				return false;
			}
		}
		return true;
	}

	/** internal, for command line utility
	 *
	 * @return */
	public static final Map<String, BdbjCompare> internGetInstances() {

		final Map<String, BdbjCompare> result = Create.tempMap();
		result.putAll(BdbjCompare.INSTANCES);
		return result;
	}

	private static final long readLow6AsLong(final byte[] data, final int offset) {

		return ((long) (data[offset + 0] & 255) << 40) + ((long) (data[offset + 1] & 255) << 32) + ((long) (data[offset + 2] & 255) << 24) + ((data[offset + 3] & 255) << 16)
				+ ((data[offset + 4] & 255) << 8) + ((data[offset + 5] & 255) << 0);
	}

	private static final void writeInt(final byte[] buffer, final int offset, final int value) {

		buffer[offset + 0] = (byte) (value >> 24);
		buffer[offset + 1] = (byte) (value >> 16);
		buffer[offset + 2] = (byte) (value >> 8);
		buffer[offset + 3] = (byte) value;
	}

	private static final void writeLongAsLow6(final byte[] buffer, final int offset, final long value) {

		buffer[offset + 0] = (byte) (value >> 40);
		buffer[offset + 1] = (byte) (value >> 32);
		buffer[offset + 2] = (byte) (value >> 24);
		buffer[offset + 3] = (byte) (value >> 16);
		buffer[offset + 4] = (byte) (value >> 8);
		buffer[offset + 5] = (byte) value;
	}

	private static final void writeShort(final byte[] buffer, final int offset, final short value) {

		buffer[offset + 0] = (byte) (value >> 8);
		buffer[offset + 1] = (byte) value;
	}

	private final byte[] bufferBytes;

	private Environment environment;

	private final Map<String, Database> knownDatabases;

	Database dbItem;

	Database dbItemGuid;

	Database dbItemQueue;

	/** vfs: tree -> parentLuid;linkName
	 *
	 * see cursorTree */
	Database dbTree;

	Database dbTreeIndex;

	Database dbTreeUsage;

	Database dbTail;

	private S4StoreType instanceType;

	private long sequenceLuid = 0;

	private File dataFolder;

	private final DatabaseEntry key;

	private final DatabaseEntry value;

	Transaction transactionLive;

	/** luid6
	 *
	 * ->
	 *
	 * schedule2;guidX */
	Cursor cursorItem;

	/** azimuth4;luid6
	 *
	 * ->
	 *
	 * schedule2;guidX */
	Cursor cursorItemGuid;

	/** queue:
	 *
	 * schedule2;luid6
	 *
	 * ->
	 *
	 * () */
	Cursor cursorItemQueue;

	/** tree:
	 *
	 * luid6;keyX
	 *
	 * ->
	 *
	 * mode1;modified6;targetX */
	Cursor cursorTree;

	/** ???:
	 *
	 * keyX;targetX;luid6
	 *
	 * ->
	 *
	 * () */
	Cursor cursorTreeIndex;

	/** ???:
	 *
	 * targetX;luid6;keyX
	 *
	 * ->
	 *
	 * () */
	Cursor cursorTreeUsageDoLinkSet;

	private Cursor cursorTreeUsageCheckReferenced;

	/** ???:
	 *
	 * luid6
	 *
	 * ->
	 *
	 * dataX
	 *
	 *
	 * Cleaned by life-cycle garbage collector. */
	Cursor cursorTail;

	/**
	 *
	 */
	public BdbjCompare() {

		this.bufferBytes = new byte[512 + 2048];
		assert this.bufferBytes.length >= S4Driver.TAIL_CAPACITY : "Preallocated buffer must be bigger than TAIL size limit";
		this.key = new DatabaseEntry();
		this.value = new DatabaseEntry();
		this.knownDatabases = new TreeMap<>();
	}

	@Override
	public void arsLinkDelete(final RecordBdbj container, final Guid key, final TreeLinkType mode, final long modified) throws Exception {

		this.arsLinkUpsert(container, key, mode, modified, null);
	}

	@Override
	public void arsLinkUpdate(final RecordBdbj container,
			final RecordBdbj newContainer,
			final Guid key,
			final Guid newKey,
			final TreeLinkType mode,
			final long modified,
			final Guid target) throws Exception {

		this.arsLinkUpsert(newContainer, newKey, mode, modified, target);
		this.arsLinkUpsert(container, key, mode, modified, null);
	}

	@Override
	public void arsLinkUpsert(final RecordBdbj container, final Guid name, final TreeLinkType mode, final long modified, final Guid target) throws Exception {

		assert target == null || target.isValid() : "Expected to be valid, target=" + target + ", type=" + target.getType() + ", inline=" + target.isInline();
		/** key;target;luid;key;mode;modified;target */
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;

		/** start build key */
		final int keyLength = Guid.writeGuid(name, this.bufferBytes, 0);
		final int guidLength = Guid.writeGuid(target, this.bufferBytes, keyLength);
		BdbjCompare.writeLongAsLow6(this.bufferBytes, keyLength + guidLength, container.luid);
		System.arraycopy(this.bufferBytes, 0, this.bufferBytes, keyLength + guidLength + 6, keyLength);
		final int treeStart = keyLength + guidLength + 6 + keyLength;
		this.bufferBytes[treeStart] = (byte) mode.ordinal();
		final long modifiedValue = modified / 1000L;
		BdbjCompare.writeLongAsLow6(this.bufferBytes, treeStart + 1, modifiedValue);
		System.arraycopy(this.bufferBytes, keyLength, this.bufferBytes, treeStart + 1 + 6, guidLength);
		/** end build key */
		/** set 'tree' link */
		setTreeLink : {
			/** keyX;targetX;(luid6;keyX);mode1;modified6;targetX */
			key.setData(this.bufferBytes, keyLength + guidLength, 6 + keyLength);
			/** search for 'tree' item */
			final OperationStatus status = this.cursorTree.getSearchKey(key, value, LockMode.RMW);
			/** item found, we know now that it is an existing one, we need to check for changes
			 * here */
			if (status == OperationStatus.SUCCESS) {
				// check delete old indices
				final TreeLinkType previousMode = TreeLinkType.valueForIndex(value.getData()[0]);
				final long previousModified = BdbjCompare.readLow6AsLong(this.bufferBytes, 1) * 1000L;
				final Guid previousTarget = Guid.readGuid(value.getData(), 1 + 6);
				assert previousTarget.isValid() : "Expected to be valid!";
				/** modes and targets are all equals and not unlinking? */
				if (previousMode == mode && target != null && previousTarget.equals(target)) {
					/** link date is also unchanged? */
					if (previousModified == modifiedValue) {
						/** nothing to change */
						return;
					}
					/** update 'modified' only */
					value.setData(this.bufferBytes, treeStart, 1 + 6 + guidLength);
					assert Guid.readGuid(this.bufferBytes, treeStart + 1 + 6).isValid() : "Expected to be valid!";
					final OperationStatus writeStatus = this.cursorTree.putCurrent(value);
					if (writeStatus != OperationStatus.SUCCESS) {
						throw new RuntimeException("unexpected status: " + writeStatus);
					}
					/** that's it! not going to touch anything else */
					return;
				}
				final boolean previousIndexing = previousMode.allowsSearchIndexing();
				final boolean previousUsage = previousMode.blocksGarbageCollection() && !previousTarget.isInline();
				if (previousIndexing || previousUsage) {
					final int freeStart = treeStart + 1 + 6 + guidLength;
					/** cleaning indices */
					if (previousIndexing) {
						System.arraycopy(this.bufferBytes, 0, this.bufferBytes, freeStart, keyLength);
						final int prevLength = Guid.writeGuid(previousTarget, this.bufferBytes, freeStart + keyLength);
						System.arraycopy(this.bufferBytes, keyLength + guidLength, this.bufferBytes, freeStart + prevLength + keyLength, 6);
						/** (key;previousTarget;luid) */
						value.setData(this.bufferBytes, freeStart, keyLength + prevLength + 6);
						final OperationStatus indexStatus = this.cursorTreeIndex.getSearchKey(value, null, LockMode.RMW);
						if (indexStatus == OperationStatus.SUCCESS) {
							final OperationStatus deleteStatus = this.cursorTreeIndex.delete();
							if (deleteStatus != OperationStatus.SUCCESS) {
								throw new RuntimeException("unexpected status: " + deleteStatus);
							}
						}
					}
					/** cleaning usage */
					if (previousUsage) {
						final int prevLength = Guid.writeGuid(previousTarget, this.bufferBytes, freeStart);
						System.arraycopy(this.bufferBytes, keyLength + guidLength, this.bufferBytes, freeStart + prevLength, 6);
						System.arraycopy(this.bufferBytes, 0, this.bufferBytes, freeStart + prevLength + 6, keyLength);
						/** (previousTargetX;luid6;keyX) */
						value.setData(this.bufferBytes, freeStart, prevLength + 6 + keyLength);
						final OperationStatus indexStatus = this.cursorTreeUsageDoLinkSet.getSearchKey(value, null, LockMode.RMW);
						if (indexStatus == OperationStatus.SUCCESS) {
							final OperationStatus deleteStatus = this.cursorTreeUsageDoLinkSet.delete();
							if (deleteStatus != OperationStatus.SUCCESS) {
								throw new RuntimeException("unexpected status: " + deleteStatus);
							}
						}
					}
				}
				if (target == null) {
					final OperationStatus writeStatus = this.cursorTree.delete();
					if (writeStatus != OperationStatus.SUCCESS) {
						throw new RuntimeException("unexpected status: " + writeStatus);
					}
					return;
				}
				value.setData(this.bufferBytes, treeStart, 1 + 6 + guidLength);
				assert Guid.readGuid(this.bufferBytes, treeStart + 1 + 6).isValid() : "Expected to be valid!";
				final OperationStatus writeStatus = this.cursorTree.putCurrent(value);
				if (writeStatus != OperationStatus.SUCCESS) {
					throw new RuntimeException("unexpected status: " + writeStatus);
				}
				break setTreeLink;
			}
			/** The 'tree' item being updated does not exist. So we need to create new one! */
			if (status == OperationStatus.NOTFOUND) {
				if (target == null) {
					/** already deleted */
					return;
				}
				/** keyX;targetX;luid6;keyX;(mode1;modified6;targetX) */
				value.setData(this.bufferBytes, treeStart, 1 + 6 + guidLength);
				assert Guid.readGuid(this.bufferBytes, treeStart + 1 + 6).isValid() : "Expected to be valid!";
				final OperationStatus writeStatus = this.cursorTree.put(key, value);
				if (writeStatus != OperationStatus.SUCCESS) {
					throw new RuntimeException("unexpected status: " + writeStatus);
				}
				break setTreeLink;
			}
			throw new RuntimeException("unexpected status: " + status);
		}
		/** update indices */
		if (mode.allowsSearchIndexing()) {
			/** (keyX;targetX;luid6);keyX;mode1;modified6;targetX */
			key.setData(this.bufferBytes, 0, keyLength + guidLength + 6);
			/** () */
			value.setData(this.bufferBytes, 0, 0);
			final OperationStatus writeStatus = this.cursorTreeIndex.put(key, value);
			if (writeStatus != OperationStatus.SUCCESS) {
				throw new RuntimeException("unexpected status: " + writeStatus);
			}
		}
		/** update usage */
		if (mode.blocksGarbageCollection() && !target.isInline()) {
			/** keyX;(targetX;luid6;keyX);mode1;modified6;targetX */
			key.setData(this.bufferBytes, keyLength, guidLength + 6 + keyLength);
			/** () */
			value.setData(this.bufferBytes, 0, 0);
			// final OperationStatus writeStatus = this.dbTreeUsage.put( txn,
			// key, value );
			final OperationStatus writeStatus = this.cursorTreeUsageDoLinkSet.put(key, value);
			if (writeStatus != OperationStatus.SUCCESS) {
				throw new RuntimeException("unexpected status: " + writeStatus);
			}
		}
	}

	@Override
	public void arsRecordDelete(final RecordBdbj record) throws Exception {

		if (record.luid == 0) {
			/** not persistent anyway! */
			return;
		}

		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;

		{
			/** azimuth4;luid6;schedule2;guidX */
			BdbjCompare.writeInt(this.bufferBytes, 0, record.guid.hashCode());
			BdbjCompare.writeLongAsLow6(this.bufferBytes, 4, record.luid);
			BdbjCompare.writeShort(this.bufferBytes, 4 + 6, record.scheduleBits);
			final int guidSize = Guid.writeGuid(record.guid, this.bufferBytes, 4 + 6 + 2);

			// data ready
			{
				/** azimuth4;(luid6);schedule2;guidX */
				key.setData(this.bufferBytes, 4, 6);

				final OperationStatus status = this.cursorItem.getSearchKey(key, value, LockMode.RMW);
				if (status == OperationStatus.NOTFOUND) {
					/** nice, no record anyway... but strange, cause we have 'luid'? */
					return;
				}
				if (status != OperationStatus.SUCCESS) {
					throw new RuntimeException("unexpected status: " + status);
				}
				{
					// check delete old indices
					final byte[] valueBytes = value.getData();
					final short previousScheduleBits = (short) (((valueBytes[0] & 0xFF) << 8) + (valueBytes[1] & 0xFF));
					final Guid previousGuid = Guid.readGuid(valueBytes, 2);
					assert previousGuid.isValid() : "Expected to be valid!";
					assert previousGuid.equals(record.guid) : "Supposed to be equal!";
					// delete indices
					final int freeStart = 4 + 6 + 2 + guidSize;
					{
						/** ...(prevAzimuth:4;luid:6) */
						BdbjCompare.writeInt(this.bufferBytes, freeStart, record.guid.hashCode());
						System.arraycopy(this.bufferBytes, 4, this.bufferBytes, freeStart + 4, 6);
						value.setData(this.bufferBytes, freeStart, 4 + 6);
						final OperationStatus indexStatus = this.cursorItemGuid.getSearchKey(value, null, LockMode.RMW);
						if (indexStatus == OperationStatus.SUCCESS) {
							final OperationStatus deleteStatus = this.cursorItemGuid.delete();
							if (deleteStatus != OperationStatus.SUCCESS) {
								throw new RuntimeException("unexpected status: " + deleteStatus);
							}
						}
					}
					{
						/** ... queue: (prevSchedule:2;luid:6) */
						BdbjCompare.writeShort(this.bufferBytes, freeStart, previousScheduleBits);
						System.arraycopy(this.bufferBytes, 4, this.bufferBytes, freeStart + 2, 6);
						value.setData(this.bufferBytes, freeStart, 2 + 6);
						final OperationStatus indexStatus = this.cursorItemQueue.getSearchKey(value, null, LockMode.RMW);
						if (indexStatus == OperationStatus.SUCCESS) {
							final OperationStatus deleteStatus = this.cursorItemQueue.delete();
							if (deleteStatus != OperationStatus.SUCCESS) {
								throw new RuntimeException("unexpected status: " + deleteStatus);
							}
						}
					}
					final OperationStatus writeStatus = this.cursorItem.delete();
					if (writeStatus != OperationStatus.SUCCESS) {
						throw new RuntimeException("unexpected status: " + writeStatus);
					}
				}
			}
		}
		if (record.guid.isBinary() && !record.guid.isInline() && record.guid.getBinaryLength() <= S4Driver.TAIL_CAPACITY) {
			/** azimuth4;schedule2;(luid6);schedule2;guidX */
			key.setData(this.bufferBytes, 4, 6);
			final OperationStatus tailStatus = this.cursorTail.getSearchKey(key, value, LockMode.RMW);
			//
			if (tailStatus == OperationStatus.SUCCESS) {
				final OperationStatus deleteStatus = this.cursorTail.delete();
				if (deleteStatus != OperationStatus.SUCCESS) {
					throw new RuntimeException("(dropTail) unexpected status: " + deleteStatus);
				}
			}
		}
		if (record.guid.isCollection()) {
			System.out.println(">>> delete container tree");
		}
	}

	@Override
	public void arsRecordUpsert(final RecordBdbj record) throws Exception {

		if (record.luid == 0) {
			record.luid = this.sequenceLuid++;
		}

		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;

		{
			/** azimuth4;luid6;schedule2;guidX */
			BdbjCompare.writeInt(this.bufferBytes, 0, record.guid.hashCode());
			BdbjCompare.writeLongAsLow6(this.bufferBytes, 4, record.luid);
			BdbjCompare.writeShort(this.bufferBytes, 4 + 6, record.scheduleBits);
			final int guidSize = Guid.writeGuid(record.guid, this.bufferBytes, 4 + 6 + 2);

			// data ready
			setItemRecord : {
				/** azimuth4;(luid6);schedule2;guidX */
				key.setData(this.bufferBytes, 4, 6);

				final OperationStatus status = this.cursorItem.getSearchKey(key, value, LockMode.RMW);
				if (status == OperationStatus.SUCCESS) {
					// check delete old indices
					final byte[] valueBytes = value.getData();
					final short previousScheduleBits = (short) (((valueBytes[0] & 0xFF) << 8) + (valueBytes[1] & 0xFF));
					final Guid previousGuid = Guid.readGuid(valueBytes, 2);
					assert previousGuid.isValid() : "Expected to be valid!";
					if (previousScheduleBits == record.scheduleBits && previousGuid.equals(record.guid)) {
						/** nothing to change, templates (like 'tail') must already be commited for
						 * an existing record. */
						return;
					}
					// delete old indices
					final int freeStart = 4 + 6 + 2 + guidSize;
					/** is it really possible? */
					if (!previousGuid.equals(record.guid)) {
						BdbjCompare.writeInt(this.bufferBytes, freeStart, previousGuid.hashCode());
						System.arraycopy(this.bufferBytes, 4, this.bufferBytes, freeStart + 4, 6);
						/** (prevAzimuth:4;luid:6) */
						value.setData(this.bufferBytes, freeStart, 4 + 6);
						final OperationStatus indexStatus = this.cursorItemGuid.getSearchKey(value, null, LockMode.RMW);
						if (indexStatus == OperationStatus.SUCCESS) {
							final OperationStatus deleteStatus = this.cursorItemGuid.delete();
							if (deleteStatus != OperationStatus.SUCCESS) {
								throw new RuntimeException("unexpected status: " + deleteStatus);
							}
						}
					}
					/** that's more likely */
					if (previousScheduleBits != record.scheduleBits) {
						BdbjCompare.writeShort(this.bufferBytes, freeStart, previousScheduleBits);
						System.arraycopy(this.bufferBytes, 4, this.bufferBytes, freeStart + 2, 6);
						/** queue: prevSchedule:2;luid:6 */
						value.setData(this.bufferBytes, freeStart, 2 + 6);
						final OperationStatus indexStatus = this.cursorItemQueue.getSearchKey(value, null, LockMode.RMW);
						if (indexStatus == OperationStatus.SUCCESS) {
							final OperationStatus deleteStatus = this.cursorItemQueue.delete();
							if (deleteStatus != OperationStatus.SUCCESS) {
								throw new RuntimeException("unexpected status: " + deleteStatus);
							}
						}
					}
					/** azimuth:4;luid:6;(schedule:2;guid:X) */
					value.setData(this.bufferBytes, 4 + 6, 2 + guidSize);
					assert Guid.readGuid(this.bufferBytes, 4 + 6 + 2).isValid() : "Expected to be valid!";
					final OperationStatus writeStatus = this.cursorItem.putCurrent(value);
					if (writeStatus != OperationStatus.SUCCESS) {
						throw new RuntimeException("unexpected status: " + writeStatus);
					}
					break setItemRecord;
				}
				if (status == OperationStatus.NOTFOUND) {
					/** azimuth:4;luid:6;(schedule:2;guid:X) */
					value.setData(this.bufferBytes, 4 + 6, 2 + guidSize);
					final OperationStatus writeStatus = this.cursorItem.put(key, value);
					if (writeStatus != OperationStatus.SUCCESS) {
						throw new RuntimeException("unexpected status: " + writeStatus);
					}
					break setItemRecord;
				}
				throw new RuntimeException("unexpected status: " + status);
			}
			/* setItemGuidRecord: */ {
				/** (azimuth:4;luid:6);schedule:2;guid */
				key.setData(this.bufferBytes, 0, 4 + 6);
				/** azimuth:4;luid:6;(schedule:2;guid) */
				value.setData(this.bufferBytes, 4 + 6, 2 + guidSize);
				final OperationStatus writeStatus = this.cursorItemGuid.put(key, value);
				if (writeStatus != OperationStatus.SUCCESS) {
					throw new RuntimeException("unexpected status: " + writeStatus);
				}
			}
			/* setItemQueueRecord: */ {
				/** azimuth:4;luid:6;schedule:2;guid
				 *
				 * ->
				 *
				 * pad:2;schedule:2;luid:6;schedule:2;guid */
				BdbjCompare.writeShort(this.bufferBytes, 2, record.scheduleBits);
				/** pad:2;(schedule:2;luid:6);schedule:2;guid */
				key.setData(this.bufferBytes, 2, 2 + 6);
				/** () */
				value.setData(this.bufferBytes, 0, 0);
				final OperationStatus writeStatus = this.cursorItemQueue.put(key, value);
				if (writeStatus != OperationStatus.SUCCESS) {
					throw new RuntimeException("unexpected status: " + writeStatus);
				}
			}
		}
		recordsAttachement : {
			if (record.guid.isBinary() && !record.guid.isInline() && record.guid.getBinaryLength() <= S4Driver.TAIL_CAPACITY) {
				final TransferCopier tail = record instanceof final RecTemplate recTemplate
					? (TransferCopier) recTemplate.getAttachment()
					: null;
				if (tail != null) {
					/** pad2;schedule2;luid6;schedule2;guidX -> pad2;schedule2;luid6;dataX */
					final int tailSize = tail.copy(0, this.bufferBytes, 4 + 6, S4Driver.TAIL_CAPACITY);
					/** pad2;schedule2;(luid6);schedule2;guidX */
					key.setData(this.bufferBytes, 4, 6);

					final OperationStatus status = this.cursorTail.getSearchKey(key, value, LockMode.RMW);
					if (status == OperationStatus.SUCCESS) {
						final byte[] valueBytes = value.getData();

						compare : {
							if (valueBytes.length != tailSize) {
								break compare;
							}
							for (int i = tailSize - 1; i >= 0; --i) {
								if (valueBytes[i] != this.bufferBytes[4 + 6 + i]) {
									break compare;
								}
							}
							break recordsAttachement;
						}

						/** pad2;schedule2;luid6;(dataX) */
						value.setData(this.bufferBytes, 4 + 6, tailSize);

						final OperationStatus writeStatus = this.cursorTail.putCurrent(value);
						if (writeStatus != OperationStatus.SUCCESS) {
							throw new RuntimeException("(setTail) unexpected status: " + writeStatus);
						}
						break recordsAttachement;
					}
					if (status == OperationStatus.NOTFOUND) {

						/** pad2;schedule2;luid6;(dataX) */
						value.setData(this.bufferBytes, 4 + 6, tailSize);

						final OperationStatus writeStatus = this.cursorTail.put(key, value);
						if (writeStatus != OperationStatus.SUCCESS) {
							throw new RuntimeException("(setTail) unexpected status: " + writeStatus);
						}
						break recordsAttachement;
					}
					throw new RuntimeException("(setTail) unexpected status: " + status);
				}
				break recordsAttachement;
			}
			if (record.guid.isCollection()) {
				@SuppressWarnings("unchecked")
				final Collection<ReferenceBdbj> tree = record instanceof final RecTemplate recTemplate
					? (Collection<ReferenceBdbj>) recTemplate.getAttachment()
					: null;
				if (tree != null) {
					for (final ReferenceBdbj impl : tree) {
						this.arsLinkUpsert(record, impl.key, impl.mode, impl.modified, impl.value);
					}
				}
				break recordsAttachement;
			}
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
	}

	@Override
	public RepairChecker createCheckContext(final BaseObject properties) {

		return new RepairChecker() {

			@Override
			public boolean next() {

				return false;
			}
		};
	}

	@Override
	public S4WorkerInterface<RecordBdbj, ReferenceBdbj, Long> createGlobalCommonTransaction() {

		return new S4WorkerTransactionDummy<>(this);
	}

	@Override
	public S4WorkerTransaction<RecordBdbj, ReferenceBdbj, Long> createNewWorkerTransaction() {

		return new S4WorkerTransactionDummy<>(this);
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

		return this;
	}

	/** @return */
	@ReflectionExplicit
	@ReflectionEnumerable
	public String getClassName() {

		return this.getClass().getSimpleName();
	}

	@Override
	public String getKey() {

		return "bdbjc";
	}

	/** @return */
	@ReflectionExplicit
	@ReflectionEnumerable
	public long getSequenceLuid() {

		return this.sequenceLuid;
	}

	/** @return */
	@ReflectionExplicit
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

	/** sync, optional. */
	public void internStorageSync() {

		this.environment.sync();
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

	private final long lastLuid(final Cursor cursor) {

		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		final OperationStatus status = cursor.getLast(key, value, null);
		if (status == OperationStatus.NOTFOUND) {
			return -1;
		}
		if (status == OperationStatus.SUCCESS) {
			return BdbjCompare.readLow6AsLong(key.getData(), 0);
		}
		throw new IllegalStateException("Invalid status: " + status);
	}

	@Override
	public int readCheckReferenced(final BasicQueue<RecordBdbj> pendingRecords, final BasicQueue<RecordBdbj> targetReferenced, final BasicQueue<RecordBdbj> targetUnreferenced)
			throws Exception {

		final byte[] bufferBytes = new byte[64];
		final DatabaseEntry key = new DatabaseEntry();
		int count = 0;
		try (final Cursor cursorTreeUsage = this.dbTreeUsage.openCursor(null, CursorConfig.READ_COMMITTED)) {
			records : for (RecordBdbj record; (record = pendingRecords.pollFirst()) != null;) {
				final int guidSize = Guid.writeGuid(record.guid, bufferBytes, 0);
				key.setData(bufferBytes, 0, guidSize);
				final OperationStatus status = cursorTreeUsage.getSearchKeyRange(key, null, LockMode.DEFAULT);
				++count;
				if (status == OperationStatus.SUCCESS) {
					targetReferenced.offerLast(record);
					continue records;
				}
				if (status == OperationStatus.NOTFOUND) {
					targetUnreferenced.offerLast(record);
					continue records;
				}
				throw new RuntimeException("unexpected status: " + status);
			}
		}
		return count;
	}

	@Override
	public int readContainerContentsRange(final Function<ReferenceBdbj, ?> target,
			final RecordBdbj record,
			final Guid keyStart,
			final Guid keyStop,
			final int limit,
			final boolean backwards) throws Exception {

		assert record != null : "Record is null";
		final long luid = record.luid;
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		if (keyStart == null) {
			BdbjCompare.writeLongAsLow6(
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
			BdbjCompare.writeLongAsLow6(this.bufferBytes, 0, luid);
			final int keyLength = Guid.writeGuid(keyStart, this.bufferBytes, 6);
			/** (luid6,ketStartX) */
			key.setData(this.bufferBytes, 0, 6 + keyLength);
		}
		final Cursor cursor = this.cursorTree;
		{
			OperationStatus status = cursor.getSearchKeyRange(key, value, LockMode.DEFAULT);
			prepareBackwards : if (backwards) {
				if (status == OperationStatus.NOTFOUND) {
					/** This means the record we are looking for is LAST in the table! Rare case
					 * 8-) */
					status = cursor.getLast(key, value, LockMode.DEFAULT);
					break prepareBackwards;
				}
				if (status == OperationStatus.SUCCESS) {
					/** now the cursor is either on the record or on the next one */
					final byte[] keyData = key.getData();
					if (BdbjCompare.readLow6AsLong(keyData, 0) != luid || keyStart != null && Guid.readCompare(keyData, 6, keyStart) != 0) {
						status = cursor.getPrev(key, value, LockMode.DEFAULT);
					}
					break prepareBackwards;
				}
				throw new RuntimeException("unexpected status: " + status);
			}
			for (int left = limit;;) {
				if (status == OperationStatus.NOTFOUND) {
					return limit - left;
				}
				if (status != OperationStatus.SUCCESS) {
					throw new RuntimeException("unexpected status: " + status);
				}
				final byte[] keyData = key.getData();
				if (BdbjCompare.readLow6AsLong(keyData, 0) != luid) {
					return limit - left;
				}
				if (keyStop != null && (backwards
					? Guid.readCompare(keyData, 6, keyStop) <= 0
					: Guid.readCompare(keyData, 6, keyStop) >= 0)) {
					return limit - left;
				}
				{
					final byte[] bytes = value.getData();
					final ReferenceBdbj reference = new ReferenceBdbj();
					reference.key = Guid.readGuid(keyData, 6);
					reference.mode = TreeLinkType.valueForIndex(bytes[0]);
					reference.modified = BdbjCompare.readLow6AsLong(bytes, 1) * 1000L;
					reference.value = Guid.readGuid(bytes, 1 + 6);
					assert reference.value.isValid() : "Stored link target expected to be valid!";
					target.apply(reference);
				}
				if (--left == 0) {
					return limit - left;
				}
				status = backwards
					? cursor.getPrev(key, value, LockMode.DEFAULT)
					: cursor.getNext(key, value, LockMode.DEFAULT);
			}
		}
	}

	@Override
	public ReferenceBdbj readContainerElement(final RecordBdbj record, final Guid name) throws Exception {

		BdbjCompare.writeLongAsLow6(this.bufferBytes, 0, record.luid);
		final int keyLength = Guid.writeGuid(name, this.bufferBytes, 6);
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		/** (luid6,keyX) */
		key.setData(this.bufferBytes, 0, 6 + keyLength);

		final OperationStatus status = this.cursorTree.getSearchKey(key, value, LockMode.DEFAULT);
		if (status == OperationStatus.SUCCESS) {
			final byte[] bytes = value.getData();
			final ReferenceBdbj result = new ReferenceBdbj();
			result.key = name;
			result.mode = TreeLinkType.valueForIndex(bytes[0]);
			result.modified = BdbjCompare.readLow6AsLong(bytes, 1) * 1000L;
			result.value = Guid.readGuid(bytes, 1 + 6);
			assert result.value.isValid() : "Stored link target expected to be valid, name: " + name + " (" + name.getInlineValue() + "), target:" + result.value;
			return result;
		}
		if (status == OperationStatus.NOTFOUND) {
			return null;
		}
		throw new RuntimeException("unexpected status: " + status);
	}

	@Override
	public RecordBdbj readRecord(final Guid guid) throws Exception {

		BdbjCompare.writeInt(this.bufferBytes, 0, guid.hashCode());
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		/** azimuth:4 */
		key.setData(this.bufferBytes, 0, 4);

		for (OperationStatus status = this.cursorItemGuid.getSearchKeyRange(key, value, null);;) {
			if (status == OperationStatus.NOTFOUND) {
				return null;
			}
			if (status != OperationStatus.SUCCESS) {
				throw new RuntimeException("unexpected status: " + status);
			}
			final byte[] bytesKey = key.getData();
			assert bytesKey.length == 4 + 6 : "key in itemGuid table should be exactly 4+6 bytes in size";
			if (!BdbjCompare.compareBytes(this.bufferBytes, 0, bytesKey, 0, 4)) {
				return null;
			}
			final byte[] bytes = value.getData();
			if (Guid.readEquals(bytes, 2, guid)) {
				final RecordBdbj result = new RecordBdbj();
				result.guid = guid;
				result.luid = BdbjCompare.readLow6AsLong(bytesKey, 4);
				result.scheduleBits = (short) (((bytes[0] & 0xFF) << 8) + (bytes[1] & 0xFF));
				return result;
			}
			status = this.cursorItemGuid.getNext(key, value, LockMode.DEFAULT);
		}
	}

	@Override
	public byte[] readRecordTail(final RecordBdbj record) throws Exception {

		BdbjCompare.writeLongAsLow6(this.bufferBytes, 0, record.luid);
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		/** (luid6) */
		key.setData(this.bufferBytes, 0, 6);

		final OperationStatus status = this.cursorTail.getSearchKey(key, value, LockMode.DEFAULT);
		if (status == OperationStatus.SUCCESS) {
			/** no 'clone' needed: For a DatabaseEntry that is used as an output parameter, the byte
			 * array will always be a newly allocated array. */
			assert !value.getPartial() : "result is promised to be an exact byte array";
			return value.getData();
		}
		if (status == OperationStatus.NOTFOUND) {
			return null;
		}
		throw new RuntimeException("unexpected status: " + status);
	}

	@Override
	public void reset() {

		//
	}

	@Override
	public int searchBetween(final Function<Long, ?> target, final Guid name, final Guid value1, final Guid value2, final int limit) throws Exception {

		final int keyLength = Guid.writeGuid(name, this.bufferBytes, 0);
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		{
			final int guidLength = Guid.writeGuid(value1, this.bufferBytes, keyLength);
			key.setData(this.bufferBytes, 0, keyLength + guidLength);
		}

		for (int left = limit;;) {
			OperationStatus status = this.cursorTreeIndex.getSearchKeyRange(key, value, LockMode.DEFAULT);
			if (null == status || status == OperationStatus.NOTFOUND) {
				return limit - left;
			}
			if (status != OperationStatus.SUCCESS) {
				throw new RuntimeException("unexpected status: " + status);
			}
			{
				final byte[] bytes = key.getData();
				if (!BdbjCompare.compareBytes(bytes, 0, this.bufferBytes, 0, keyLength) || Guid.readCompare(bytes, keyLength, value2) > 0) {
					return limit - left;
				}
				final long luid = BdbjCompare.readLow6AsLong(bytes, keyLength + Guid.readGuidByteCount(bytes, keyLength));
				target.apply(Long.valueOf(luid));
			}
			if (--left == 0) {
				return limit;
			}
			status = this.cursorTreeIndex.getNext(key, value, LockMode.DEFAULT);
		}
	}

	@Override
	public int searchEquals(final Collection<Long> target, final Guid name, final Guid value1, final int limit) throws Exception {

		final int keyLength = Guid.writeGuid(name, this.bufferBytes, 0);
		final DatabaseEntry key = this.key;
		final DatabaseEntry value = this.value;
		final int guidLength = Guid.writeGuid(value1, this.bufferBytes, keyLength);
		/** (key;target)[;luid?] */
		key.setData(this.bufferBytes, 0, keyLength + guidLength);

		for (int left = limit;;) {
			OperationStatus status = this.cursorTreeIndex.getSearchKeyRange(key, value, LockMode.DEFAULT);
			if (null == status || status == OperationStatus.NOTFOUND) {
				return limit - left;
			}
			if (status != OperationStatus.SUCCESS) {
				throw new RuntimeException("unexpected status: " + status);
			}
			{
				final byte[] bytes = key.getData();
				if (!BdbjCompare.compareBytes(bytes, 0, this.bufferBytes, 0, keyLength + guidLength)) {
					return limit - left;
				}
				final long luid = BdbjCompare.readLow6AsLong(bytes, keyLength + guidLength);
				target.add(Long.valueOf(luid));
			}
			if (--left == 0) {
				return limit;
			}
			status = this.cursorTreeIndex.getNext(key, value, LockMode.DEFAULT);
		}
	}

	@Override
	public void searchScheduled(final short scheduleBits, final int limit, final BasicQueue<RecordBdbj> targetScheduled) throws Exception {

		final byte[] bufferBytes = new byte[2 + 6];
		BdbjCompare.writeShort(bufferBytes, 0, scheduleBits);
		final DatabaseEntry key = new DatabaseEntry();
		final DatabaseEntry value = new DatabaseEntry();
		key.setData(bufferBytes, 0, 2);
		try (final Cursor cursorItemQueue = this.dbItemQueue.openCursor(null, null); //
				final Cursor cursorItem = this.dbItem.openCursor(null, null); //
		) {
			OperationStatus status = cursorItemQueue.getSearchKeyRange(key, value, LockMode.DEFAULT);
			for (int left = limit;;) {
				if (status == OperationStatus.NOTFOUND) {
					return;
				}
				if (status != OperationStatus.SUCCESS) {
					throw new RuntimeException("unexpected status: " + status);
				}
				{
					final byte[] usageBytes = key.getData();
					if (!BdbjCompare.compareBytes(usageBytes, 0, bufferBytes, 0, 2)) {
						return;
					}
					final RecordBdbj record = new RecordBdbj();
					record.luid = BdbjCompare.readLow6AsLong(usageBytes, 2);

					key.setData(usageBytes, 2, 6);
					final OperationStatus statusItem = cursorItem.getSearchKey(key, value, LockMode.DEFAULT);
					if (statusItem == OperationStatus.NOTFOUND) {
						status = cursorItemQueue.delete();
						if (status != OperationStatus.SUCCESS) {
							throw new RuntimeException("unexpected status: " + status);
						}
					} else //
					if (statusItem != OperationStatus.SUCCESS) {
						throw new RuntimeException("unexpected status: " + statusItem);
					} else {
						record.guid = Guid.readGuid(value.getData(), 2);
						record.scheduleBits = scheduleBits;
						targetScheduled.offerLast(record);
					}
					if (--left == 0) {
						return;
					}
				}
				status = cursorItemQueue.getNext(key, value, LockMode.DEFAULT);
			}
		}
	}

	@Override
	public void setup(final S4StoreType type) throws Exception {

		this.instanceType = type;
		final File folder = new File(Engine.PATH_PRIVATE, "data");
		this.dataFolder = new File(folder, BdbjCompare.BASE_NAME + type.toString());
	}

	@Override
	public void start() throws Exception {

		this.dataFolder.mkdirs();
		retry : for (;;) {
			System.out.println("S4VFS-IMPL: BDBJE: creating environment...");
			final EnvironmentConfig config = new EnvironmentConfig();
			config.setAllowCreate(true);
			config.setLockTimeout(120L, TimeUnit.SECONDS);
			config.setReadOnly(false);
			config.setLocking(true); // should be true for auto-cleaner
			config.setTransactional(false);
			config.setTxnTimeout(300L, TimeUnit.SECONDS);
			config.setDurability(Durability.COMMIT_WRITE_NO_SYNC);
			config.setSharedCache(true);
			config.setCachePercent(this.instanceType.defaultCachePercent());

			config.setConfigParam(EnvironmentConfig.CLEANER_BACKGROUND_PROACTIVE_MIGRATION, "true");
			config.setConfigParam(EnvironmentConfig.CLEANER_LAZY_MIGRATION, "true");
			config.setConfigParam(EnvironmentConfig.CLEANER_THREADS, "2");

			final String name = BdbjCompare.BASE_NAME + this.instanceType.toString();

			config.setExceptionListener(new ExceptionListener() {

				@Override
				public void exceptionThrown(final ExceptionEvent event) {

					BdbjCompare.LOG.event("BDBJ-LCL", "EXCEPTION:" + name + ':' + event.getThreadName(), Format.Throwable.toText(event.getException()));
				}
			});

			config.setLoggingHandler(new Handler() {

				@Override
				public void close() throws SecurityException {

					BdbjCompare.LOG.event("BDBJ-LCL", "MESSAGE:" + name, "Log closed.");
				}

				@Override
				public void flush() {

					// ignore
				}

				@Override
				public void publish(final LogRecord record) {

					BdbjCompare.LOG.event("BDBJ-LCL", BdbjCompare.this.toString(), record.getMessage());
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
			{
				System.out.println("S4VFS-IMPL: BDBJE: opening 'item' database...");
				final DatabaseConfig config = new DatabaseConfig();
				config.setAllowCreate(true);
				config.setExclusiveCreate(false);
				config.setReadOnly(false);
				config.setTransactional(false);
				config.setSortedDuplicates(false);
				// config.setDeferredWrite( true );
				this.dbItem = this.environment.openDatabase(null, "item", config);
				this.knownDatabases.put("item", this.dbItem);
			}
			{
				System.out.println("S4VFS-IMPL: BDBJE: opening 'guid' database...");
				final DatabaseConfig config = new DatabaseConfig();
				config.setAllowCreate(true);
				config.setExclusiveCreate(false);
				config.setReadOnly(false);
				config.setTransactional(false);
				config.setSortedDuplicates(true);
				this.dbItemGuid = this.environment.openDatabase(null, "guid", config);
				this.knownDatabases.put("guid", this.dbItemGuid);
			}
			{
				System.out.println("S4VFS-IMPL: BDBJE: opening 'queue' secondary database...");
				final DatabaseConfig config = new DatabaseConfig();
				config.setAllowCreate(true);
				config.setExclusiveCreate(false);
				config.setReadOnly(false);
				config.setTransactional(false);
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
				config.setTransactional(false);
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
				config.setTransactional(false);
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
				config.setTransactional(false);
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
				config.setTransactional(false);
				config.setSortedDuplicates(false);
				this.dbTail = this.environment.openDatabase(null, "tail", config);
				this.knownDatabases.put("tail", this.dbTail);
			}
		}
		{
			{
				final TransactionConfig config = new TransactionConfig();
				config.setReadCommitted(true);
				config.setDurability(Durability.READ_ONLY_TXN);
				this.transactionLive = this.environment.beginTransaction(null, config);
			}
		}
		{
			{
				System.out.println("S4VFS-IMPL: BDBJE: opening 'item' cursor...");
				this.cursorItem = this.dbItem.openCursor(
						this.transactionLive, //
						CursorConfig.READ_COMMITTED);
			}
			{
				System.out.println("S4VFS-IMPL: BDBJE: opening 'itemGuid' cursor...");
				this.cursorItemGuid = this.dbItemGuid.openCursor(
						this.transactionLive, //
						CursorConfig.READ_COMMITTED);
			}
			{
				System.out.println("S4VFS-IMPL: BDBJE: opening 'itemQueue' cursor...");
				this.cursorItemQueue = this.dbItemQueue.openCursor(
						this.transactionLive, //
						CursorConfig.READ_COMMITTED);
			}
			{
				System.out.println("S4VFS-IMPL: BDBJE: opening 'tree' cursor...");
				this.cursorTree = this.dbTree.openCursor(
						this.transactionLive, //
						CursorConfig.READ_COMMITTED);
			}
			if (this.instanceType.storeIndex()) {
				System.out.println("S4VFS-IMPL: BDBJE: opening 'treeIndex' cursor...");
				this.cursorTreeIndex = this.dbTreeIndex.openCursor(
						this.transactionLive, //
						CursorConfig.READ_COMMITTED);
			}
			if (this.instanceType.storeUsage()) {
				System.out.println("S4VFS-IMPL: BDBJE: opening 'treeUsage' cursor...");
				// this one must be opened in 'checkReferenced's thread
				this.cursorTreeUsageCheckReferenced = null;
				this.cursorTreeUsageDoLinkSet = this.dbTreeUsage.openCursor(
						this.transactionLive, //
						CursorConfig.READ_COMMITTED);
			}
			{
				System.out.println("S4VFS-IMPL: BDBJE: opening 'tail' cursor...");
				this.cursorTail = this.dbTail.openCursor(
						this.transactionLive, //
						CursorConfig.READ_COMMITTED);
			}
		}
		{
			System.out.println("S4VFS-IMPL: BDBJE: reading latest sequence luid...");
			long greatestLuid = 0;
			{
				final long luid = this.lastLuid(this.cursorItem);
				greatestLuid = Math.max(greatestLuid, luid);
				System.out.println(
						"S4VFS-IMPL: BDBJE: 'item' database: " + (luid == -1
							? "empty table"
							: luid + ", max=" + greatestLuid));
			}
			{
				final long luid = this.lastLuid(this.cursorTree);
				greatestLuid = Math.max(greatestLuid, luid);
				System.out.println(
						"S4VFS-IMPL: BDBJE: 'tree' database: " + (luid == -1
							? "empty table"
							: luid + ", max=" + greatestLuid));
			}
			{
				final long luid = this.lastLuid(this.cursorTail);
				greatestLuid = Math.max(greatestLuid, luid);
				System.out.println(
						"S4VFS-IMPL: BDBJE: 'tail' database: " + (luid == -1
							? "empty table"
							: luid + ", max=" + greatestLuid));
			}
			this.sequenceLuid = greatestLuid + 1;
			System.out.println("S4VFS-IMPL: BDBJE: luid sequence will start with: " + this.sequenceLuid);
		}
		BdbjCompare.INSTANCES.put(this.instanceType.toString(), this);
		System.out.println("S4VFS-IMPL: BDBJE: done.");
	}

	@Override
	public void stop() throws Exception {

		if (this.environment == null) {
			return;
		}
		BdbjCompare.INSTANCES.remove(this.instanceType.toString());
		if (this.cursorTail != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'tail' cursor...");
			try {
				this.cursorTail.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorTail = null;
		}
		if (this.cursorTreeUsageCheckReferenced != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'treeUsage'-CheckReferenced cursor...");
			try {
				this.cursorTreeUsageCheckReferenced.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorTreeUsageCheckReferenced = null;
		}
		if (this.cursorTreeUsageDoLinkSet != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'treeUsage'-DoLinkSet cursor...");
			try {
				this.cursorTreeUsageDoLinkSet.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorTreeUsageDoLinkSet = null;
		}
		if (this.cursorTreeIndex != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'treeIndex' cursor...");
			try {
				this.cursorTreeIndex.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorTreeIndex = null;
		}
		if (this.cursorTree != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'tree' cursor...");
			try {
				this.cursorTree.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorTree = null;
		}
		if (this.cursorItemQueue != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'itemQueue' cursor...");
			try {
				this.cursorItemQueue.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorItemQueue = null;
		}
		if (this.cursorItemGuid != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'itemGuid' cursor...");
			try {
				this.cursorItemGuid.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorItemGuid = null;
		}
		if (this.cursorItem != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing 'item' cursor...");
			try {
				this.cursorItem.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorItem = null;
		}
		if (this.transactionLive != null) {
			System.out.println("S4VFS-IMPL: BDBJE: closing live transaction...");
			try {
				this.transactionLive.abort();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.transactionLive = null;
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
}
