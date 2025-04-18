package ru.myx.ae3.vfs.s4.lcl.bdbj;

import java.util.function.Function;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.ForwardCursor;
import com.sleepycat.je.Get;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Transaction;

import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.eval.Evaluate;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.know.Guid;
import ru.myx.sapi.FormatSAPI;

enum TableDefinition {

	/** An index of non-inline -- container and binary objects (maps 'em to their guids)
	 *
	 * <item-luid6>;<queue-schedule2><item-guidX> */
	ITEM {

		@Override
		Database getDatabase(final BdbjLocalS4 local) {

			return local.dbItem;
		}

		@Override
		RowItem rowAdopt(final BaseObject description) {

			if (description instanceof final RowItem rowItem) {
				return rowItem;
			}
			if (description == null) {
				throw new NullPointerException();
			}
			final RowItem result = new RowItem();
			Base.putAll(result, description);
			if (result.key0_luid6 == 0) {
				throw new IllegalArgumentException("all fields are required, source: " + FormatSAPI.jsDescribe(description) + ", target: " + FormatSAPI.jsObject(result));
			}
			if (result.val1_guidX == null) {
				throw new IllegalArgumentException("all fields are required, source: " + FormatSAPI.jsDescribe(description) + ", target: " + FormatSAPI.jsObject(result));
			}
			return result;
		}

		@Override
		RowItem rowMaterialize(final byte[] key, final byte[] value) {

			return RowItem.materializeRowItem(new RowItem(), key, value);
		}

		@Override
		void setupRangeKeyStart(final DatabaseEntry key, final String filterDesc) {

			if (filterDesc.startsWith("key0_luid6:") || filterDesc.startsWith("luid:")) {
				final long luid = Long.parseLong(filterDesc.substring(filterDesc.indexOf(':') + 1));
				final byte[] bytes = new byte[6];
				WorkerBdbj.writeLongAsLow6(bytes, 0, luid);
				key.setData(bytes, 0, 6);
				return;
			}
			throw new IllegalArgumentException("Can't upderstand key expression: " + filterDesc);
		}

		@Override
		void storeRecord(final BdbjLocalS4 local, final Transaction txn, final BaseObject description) {

			final RowItem item = this.rowAdopt(description);
			try (Cursor cursor = local.dbItem.openCursor(txn, CursorConfig.READ_COMMITTED)) {
				RowItem.serializeRowItem(cursor, new byte[2048], new DatabaseEntry(), new DatabaseEntry(), item.key0_luid6, item.val0_schedule2, item.val1_guidX);
			}
		}
	},
	/** An index of non-inline -- container and binary objects (maps 'em to luids)
	 *
	 * <guid-azimuth4><item-luid6>;<queue-schedule2><item-guidX> */
	ITEM_GUID {

		@Override
		Database getDatabase(final BdbjLocalS4 local) {

			return local.dbItemGuid;
		}

		@Override
		RowItemGuid rowAdopt(final BaseObject description) {

			if (description instanceof final RowItemGuid rowItemGuid) {
				return rowItemGuid;
			}
			if (description == null) {
				throw new NullPointerException();
			}
			final RowItemGuid result = new RowItemGuid();
			Base.putAll(result, description);
			if (result.key1_luid6 == 0) {
				throw new IllegalArgumentException("all fields are required, source: " + FormatSAPI.jsDescribe(description) + ", target: " + FormatSAPI.jsObject(result));
			}
			if (result.val1_guidX == null) {
				throw new IllegalArgumentException("all fields are required, source: " + FormatSAPI.jsDescribe(description) + ", target: " + FormatSAPI.jsObject(result));
			}
			if (result.key0_azimuth4 != result.val1_guidX.hashCode()) {
				throw new IllegalArgumentException(
						"all fields are required, source: " + FormatSAPI.jsDescribe(description) + ", azimuth is wrong: " + result.val1_guidX.hashCode() + " expected");
			}
			return result;
		}

		@Override
		RowItemGuid rowMaterialize(final byte[] key, final byte[] value) {

			return RowItemGuid.materializeRowItemGuid(new RowItemGuid(), key, value);
		}

		@Override
		void setupRangeKeyStart(final DatabaseEntry key, final String filterDesc) {

			if (filterDesc.startsWith("azimuth+luid:") || filterDesc.startsWith("azimuth:luid:")) {
				final String filter = filterDesc.substring("azimuth+luid:".length());
				final String azimuthExpr = filter.substring(0, filter.indexOf(':'));
				final String luidExpr = filter.substring(filter.indexOf(':') + 1);

				final int azimuth = Evaluate.evaluateObject(azimuthExpr, Exec.currentProcess(), null).baseToJavaInteger();
				final long luid = Long.parseLong(luidExpr);

				final byte[] bytes = new byte[4 + 6];
				WorkerBdbj.writeInt(bytes, 0, azimuth);
				WorkerBdbj.writeLongAsLow6(bytes, 4, luid);
				key.setData(bytes, 0, bytes.length);
				return;
			}
			if (filterDesc.startsWith("luid+guid:") || filterDesc.startsWith("luid:guid:")) {
				final String filter = filterDesc.substring("luid+guid:".length());
				final String luidExpr = filter.substring(0, filter.indexOf(':'));
				final String guidExpr = filter.substring(filter.indexOf(':') + 1);

				final Guid guid = Guid.forUnknown(Evaluate.evaluateObject(guidExpr, Exec.currentProcess(), null));
				final long luid = Long.parseLong(luidExpr);

				final byte[] bytes = new byte[4 + 6];
				WorkerBdbj.writeInt(bytes, 0, guid.hashCode());
				WorkerBdbj.writeLongAsLow6(bytes, 4, luid);
				key.setData(bytes, 0, bytes.length);
				return;
			}
			if (filterDesc.startsWith("key0_azimuth4:") || filterDesc.startsWith("azimuth:")) {
				final String expr = filterDesc.substring(filterDesc.indexOf(':') + 1);

				final int azimuth = Evaluate.evaluateObject(expr, Exec.currentProcess(), null).baseToJavaInteger();

				final byte[] bytes = new byte[4];
				WorkerBdbj.writeInt(bytes, 0, azimuth);
				key.setData(bytes, 0, 4);
				return;
			}
			if (filterDesc.startsWith("guid:")) {
				final String guidExpr = filterDesc.substring(filterDesc.indexOf(':') + 1);

				final Guid guid = Guid.forUnknown(Evaluate.evaluateObject(guidExpr, Exec.currentProcess(), null));

				final byte[] bytes = new byte[4];
				WorkerBdbj.writeInt(bytes, 0, guid.hashCode());
				key.setData(bytes, 0, 4);
				return;
			}
			throw new IllegalArgumentException("Can't upderstand key expression: " + filterDesc);
		}

		@Override
		void storeRecord(final BdbjLocalS4 local, final Transaction txn, final BaseObject description) {

			final RowItemGuid item = this.rowAdopt(description);
			try (Cursor cursor = local.dbItemGuid.openCursor(txn, CursorConfig.READ_COMMITTED)) {
				RowItemGuid.serializeRowItemGuid(
						cursor,
						new byte[2048],
						new DatabaseEntry(),
						new DatabaseEntry(),
						item.key0_azimuth4,
						item.key1_luid6,
						item.val0_schedule2,
						item.val1_guidX);
			}
		}

	},
	/** An circular queue for scheduled item tasks (checks and maintenance):
	 *
	 * <queue-schedule2><item-luid6>;<> */
	ITEM_QUEUE {

		@Override
		Database getDatabase(final BdbjLocalS4 local) {

			return local.dbItemQueue;
		}

		@Override
		RowItemQueue rowAdopt(final BaseObject description) {

			throw new UnsupportedOperationException("Not yet!");
		}

		@Override
		RowItemQueue rowMaterialize(final byte[] key, final byte[] value) {

			return RowItemQueue.materializeRowItemQueue(new RowItemQueue(), key);
		}

		@Override
		void setupRangeKeyStart(final DatabaseEntry key, final String filterDesc) {

			if (filterDesc.startsWith("key00_schedule2:") || filterDesc.startsWith("schedule:")) {
				final long scheduleBits = Long.parseLong(filterDesc.substring(filterDesc.indexOf(':') + 1));
				final byte[] bytes = new byte[2];
				WorkerBdbj.writeShort(bytes, 0, (short) scheduleBits);
				key.setData(bytes, 0, 2);
				return;
			}
			throw new IllegalArgumentException("Can't upderstand key expression: " + filterDesc);
		}

		@Override
		void storeRecord(final BdbjLocalS4 local, final Transaction txn, final BaseObject description) {

			throw new UnsupportedOperationException("Not yet!");
		}
	},
	/** An index of non-inline but small-sized binary objects:
	 *
	 * <owner-luid6>;<binary-blob-byteX> */
	TAIL {

		@Override
		Database getDatabase(final BdbjLocalS4 local) {

			return local.dbTail;
		}

		@Override
		RowTail rowAdopt(final BaseObject description) {

			throw new UnsupportedOperationException("Not yet!");
		}

		@Override
		RowTail rowMaterialize(final byte[] key, final byte[] value) {

			return RowTail.materializeRowTail(new RowTail(), key, value);
		}

		@Override
		void setupRangeKeyStart(final DatabaseEntry key, final String filterDesc) {

			if (filterDesc.startsWith("key0_luid6:") || filterDesc.startsWith("luid:")) {
				final long luid = Long.parseLong(filterDesc.substring(filterDesc.indexOf(':') + 1));
				final byte[] bytes = new byte[6];
				WorkerBdbj.writeLongAsLow6(bytes, 0, luid);
				key.setData(bytes, 0, 6);
				return;
			}
			throw new IllegalArgumentException("Can't upderstand key expression: " + filterDesc);
		}

		@Override
		void storeRecord(final BdbjLocalS4 local, final Transaction txn, final BaseObject description) {

			throw new UnsupportedOperationException("Not yet!");
		}
	},
	/** An index of all containers' chidren:
	 *
	 * <parent-luid6><reference-name-guidX>;<reference-mode1><date-modified6><target-guidX> */
	TREE {

		@Override
		Database getDatabase(final BdbjLocalS4 local) {

			return local.dbTree;
		}

		@Override
		RowTree rowAdopt(final BaseObject description) {

			throw new UnsupportedOperationException("Not yet!");
		}

		@Override
		RowTree rowMaterialize(final byte[] key, final byte[] value) {

			return RowTree.materializeRowTree(new RowTree(), key, value);
		}

		@Override
		void setupRangeKeyStart(final DatabaseEntry key, final String filterDesc) {

			if (filterDesc.startsWith("luid+key:") || filterDesc.startsWith("luid:key:")) {
				final String filter = filterDesc.substring("luid+key:".length());
				final String luidExpr = filter.substring(0, filter.indexOf(':'));
				final String expr = filter.substring(filter.indexOf(':') + 1);

				final long luid = Long.parseLong(luidExpr);
				final Guid guid = Guid.forUnknown(Evaluate.evaluateObject(expr, Exec.currentProcess(), null));
				final int guidLength = Guid.writeGuidByteCount(guid);

				final byte[] bytes = new byte[6 + guidLength];
				WorkerBdbj.writeLongAsLow6(bytes, 0, luid);
				Guid.writeGuid(guid, bytes, 6);
				key.setData(bytes, 0, bytes.length);
				return;
			}
			if (filterDesc.startsWith("key0_luid6:") || filterDesc.startsWith("luid:")) {
				final long luid = Long.parseLong(filterDesc.substring(filterDesc.indexOf(':') + 1));
				final byte[] bytes = new byte[6];
				WorkerBdbj.writeLongAsLow6(bytes, 0, luid);
				key.setData(bytes, 0, 6);
				return;
			}
			throw new IllegalArgumentException("Can't upderstand key expression: " + filterDesc);
		}

		@Override
		void storeRecord(final BdbjLocalS4 local, final Transaction txn, final BaseObject description) {

			throw new UnsupportedOperationException("Not yet!");
		}
	},
	/** Index of searchable references:
	 *
	 * <reference-name-guidX><target-guidX><parent-luid6>;<> */
	TREE_INDEX {

		@Override
		Database getDatabase(final BdbjLocalS4 local) {

			return local.dbTreeIndex;
		}

		@Override
		RowTreeIndex rowAdopt(final BaseObject description) {

			throw new UnsupportedOperationException("Not yet!");
		}

		@Override
		RowTreeIndex rowMaterialize(final byte[] key, final byte[] value) {

			return RowTreeIndex.materializeRowTreeIndex(new RowTreeIndex(), key);
		}

		@Override
		void setupRangeKeyStart(final DatabaseEntry key, final String filterDesc) {

			if (filterDesc.startsWith("key0_keyX:") || filterDesc.startsWith("key:")) {
				final String expr = filterDesc.substring(filterDesc.indexOf(':') + 1);
				final Guid guid = Guid.forUnknown(Evaluate.evaluateObject(expr, Exec.currentProcess(), null));
				final int length = Guid.writeGuidByteCount(guid);
				final byte[] bytes = new byte[length];
				Guid.writeGuid(guid, bytes, 0);
				key.setData(bytes, 0, length);
				return;
			}
			throw new IllegalArgumentException("Can't upderstand key expression: " + filterDesc);
		}

		@Override
		void storeRecord(final BdbjLocalS4 local, final Transaction txn, final BaseObject description) {

			throw new UnsupportedOperationException("Not yet!");
		}
	},
	/** Index of non-primitive, non-inline value references:
	 *
	 * <target-guidX><parent-luid6><reference-name-guidX>;<> */
	TREE_USAGE {

		@Override
		Database getDatabase(final BdbjLocalS4 local) {

			return local.dbTreeUsage;
		}

		@Override
		RowTreeUsage rowAdopt(final BaseObject description) {

			throw new UnsupportedOperationException("Not yet!");
		}

		@Override
		RowTreeUsage rowMaterialize(final byte[] key, final byte[] value) {

			return RowTreeUsage.materializeRowTreeUsage(new RowTreeUsage(), key);
		}

		@Override
		void setupRangeKeyStart(final DatabaseEntry key, final String filterDesc) {

			if (filterDesc.startsWith("key0_targetX:") || filterDesc.startsWith("target:")) {
				final String expr = filterDesc.substring(filterDesc.indexOf(':') + 1);
				final Guid guid = Guid.forUnknown(Evaluate.evaluateObject(expr, Exec.currentProcess(), null));
				final int length = Guid.writeGuidByteCount(guid);
				final byte[] bytes = new byte[length];
				Guid.writeGuid(guid, bytes, 0);
				key.setData(bytes, 0, length);
				return;
			}
			throw new IllegalArgumentException("Can't understand start expression: " + filterDesc);
		}

		@Override
		void storeRecord(final BdbjLocalS4 local, final Transaction txn, final BaseObject description) {

			throw new UnsupportedOperationException("Not yet!");
		}
	},;

	static TableDefinition findTable(final String name) {

		switch (name.length()) {
			case 4 :
				if ("item".equals(name)) {
					return ITEM;
				}
				if ("guid".equals(name)) {
					return ITEM_GUID;
				}
				if ("tail".equals(name)) {
					return TAIL;
				}
				if ("tree".equals(name)) {
					return TREE;
				}
				break;
			case 5 :
				if ("queue".equals(name)) {
					return ITEM_QUEUE;
				}
				if ("index".equals(name)) {
					return TREE_INDEX;
				}
				if ("usage".equals(name)) {
					return TREE_USAGE;
				}
				break;
			case 8 :
				if ("itemGuid".equals(name)) {
					return ITEM_GUID;
				}
				break;
			case 9 :
				if ("itemQueue".equals(name)) {
					return ITEM_QUEUE;
				}
				if ("treeIndex".equals(name)) {
					return TREE_INDEX;
				}
				if ("treeUsage".equals(name)) {
					return TREE_USAGE;
				}
				break;
			default :
		}
		throw new IllegalArgumentException("Unknown database name: " + name);
	}

	static final boolean scanRowItemRecords(final ForwardCursor cursor, final Function<RowItem, ?> target, final int limit) throws Exception {

		int left = limit;
		final DatabaseEntry key = new DatabaseEntry();
		final DatabaseEntry value = new DatabaseEntry();
		OperationResult status = cursor.get(key, value, Get.CURRENT, null);
		for (;;) {
			if (null == status) {
				return false;
			}
			target.apply(RowItem.materializeRowItem(new RowItem(), key.getData(), value.getData()));
			if (--left == 0) {
				return true;
			}
			status = cursor.get(key, value, Get.NEXT, null);
		}
	}

	abstract Database getDatabase(BdbjLocalS4 local);

	final Cursor openCursor(final BdbjLocalS4 local, final Transaction txn) {

		return this.getDatabase(local).openCursor(txn, CursorConfig.READ_COMMITTED);
	}

	abstract BaseObject rowAdopt(BaseObject description);

	abstract BaseObject rowMaterialize(byte[] key, byte[] value) throws IndexOutOfBoundsException, IllegalArgumentException;

	abstract void setupRangeKeyStart(DatabaseEntry key, String filterDesc);

	final byte[] setupRangeKeyString(final String exactKey) {

		final DatabaseEntry key = new DatabaseEntry();
		if (exactKey == null || exactKey.isBlank()) {
			return null;
		}
		this.setupRangeKeyStart(key, exactKey);
		return key.getData();
	}

	abstract void storeRecord(BdbjLocalS4 local, final Transaction txn, BaseObject description);
}
