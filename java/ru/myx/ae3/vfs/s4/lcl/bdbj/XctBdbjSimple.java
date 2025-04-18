package ru.myx.ae3.vfs.s4.lcl.bdbj;

import java.util.Collection;
import java.util.function.Function;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Transaction;

import ru.myx.ae3.know.Guid;
import ru.myx.ae3.vfs.TreeLinkType;

class XctBdbjSimple //
		extends
			XctBdbj {

	private final WorkerBdbj parent;

	Transaction txn;

	CursorConfig cc;

	Cursor cursorTree;

	Cursor cursorTreeIndex;

	Cursor cursorTreeUsage;

	Cursor cursorItem;

	Cursor cursorItemGuid;

	Cursor cursorItemQueue;

	Cursor cursorTail;

	XctBdbjSimple(final WorkerBdbj parent, final Transaction txn, final CursorConfig cc) {

		this.parent = parent;
		this.txn = txn;
		this.cc = cc == null
			? CursorConfig.DEFAULT
			: cc;
	}

	@Override
	public void arsLinkDelete(final RecordBdbj container, final Guid key, final TreeLinkType mode, final long modified) throws Exception {

		this.parent.arsLinkDelete(this, container, key, mode, modified);
	}

	@Override
	public void arsLinkUpdate(final RecordBdbj container,
			final RecordBdbj newContainer,
			final Guid key,
			final Guid newKey,
			final TreeLinkType mode,
			final long modified,
			final Guid value) throws Exception {

		this.parent.arsLinkUpdate(this, container, newContainer, key, newKey, mode, modified, value);
	}

	@Override
	public void arsLinkUpsert(final RecordBdbj container, final Guid key, final TreeLinkType mode, final long modified, final Guid value) throws Exception {

		this.parent.arsLinkUpsert(this, container, key, mode, modified, value);
	}

	@Override
	public void arsRecordDelete(final RecordBdbj record) throws Exception {

		this.parent.arsRecordDelete(this, record);
	}

	@Override
	public void arsRecordUpsert(final RecordBdbj record) throws Exception {

		this.parent.arsRecordUpsert(this, record);
	}

	@Override
	public void cancel() throws Exception {

		if (this.txn == null) {
			return;
		}
		this.closeImpl();
		this.txn.abort();
		this.txn = null;
	}

	/**
	 *
	 */
	public void closeImpl() {

		if (this.cursorTree != null) {
			try {
				this.cursorTree.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorTree = null;
		}
		if (this.cursorTreeIndex != null) {
			try {
				this.cursorTreeIndex.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorTreeIndex = null;
		}
		if (this.cursorTreeUsage != null) {
			try {
				this.cursorTreeUsage.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorTreeUsage = null;
		}
		if (this.cursorItem != null) {
			try {
				this.cursorItem.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorItem = null;
		}
		if (this.cursorItemGuid != null) {
			try {
				this.cursorItemGuid.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorItemGuid = null;
		}
		if (this.cursorItemQueue != null) {
			try {
				this.cursorItemQueue.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorItemQueue = null;
		}
		if (this.cursorTail != null) {
			try {
				this.cursorTail.close();
			} catch (final DatabaseException e) {
				// ignore
			}
			this.cursorTail = null;
		}
	}

	@Override
	public void commit() throws Exception {

		if (this.txn == null) {
			return;
		}
		this.closeImpl();
		this.txn.commit();
		this.txn = null;
	}

	@Override
	void destroy() {

		if (this.txn == null) {
			return;
		}
		this.closeImpl();
		this.txn.abort();
		this.txn = null;
	}

	@Override
	public Cursor getCursorItem(final WorkerBdbj local) {

		if (this.cursorItem != null) {
			return this.cursorItem;
		}
		return this.cursorItem = local.dbItem.openCursor(this.txn, this.cc);
	}

	@Override
	public Cursor getCursorItemGuid(final WorkerBdbj local) {

		if (this.cursorItemGuid != null) {
			return this.cursorItemGuid;
		}
		return this.cursorItemGuid = local.dbItemGuid.openCursor(this.txn, this.cc);
	}

	@Override
	public Cursor getCursorItemQueue(final WorkerBdbj local) {

		if (this.cursorItemQueue != null) {
			return this.cursorItemQueue;
		}
		return this.cursorItemQueue = local.dbItemQueue.openCursor(this.txn, this.cc);
	}

	@Override
	public Cursor getCursorTail(final WorkerBdbj local) {

		if (this.cursorTail != null) {
			return this.cursorTail;
		}
		return this.cursorTail = local.dbTail.openCursor(this.txn, this.cc);
	}

	@Override
	public Cursor getCursorTree(final WorkerBdbj local) {

		if (this.cursorTree != null) {
			return this.cursorTree;
		}
		return this.cursorTree = local.dbTree.openCursor(this.txn, this.cc);
	}

	@Override
	public Cursor getCursorTreeIndex(final WorkerBdbj local) {

		if (this.cursorTreeIndex != null) {
			return this.cursorTreeIndex;
		}
		return this.cursorTreeIndex = local.dbTreeIndex.openCursor(this.txn, this.cc);
	}

	@Override
	public Cursor getCursorTreeUsage(final WorkerBdbj local) {

		if (this.cursorTreeUsage != null) {
			return this.cursorTreeUsage;
		}
		return this.cursorTreeUsage = local.dbTreeUsage.openCursor(this.txn, this.cc);
	}

	@Override
	public int readContainerContentsRange(final Function<ReferenceBdbj, ?> target,
			final RecordBdbj container,
			final Guid keyStart,
			final Guid keyStop,
			final int limit,
			final boolean backwards) throws Exception {

		return this.parent.readContainerContentsRange(this, target, container, keyStart, keyStop, limit, backwards);
	}

	@Override
	public ReferenceBdbj readContainerElement(final RecordBdbj container, final Guid key) throws Exception {

		return this.parent.readContainerElement(this, container, key);
	}

	@Override
	public RecordBdbj readRecord(final Guid guid) throws Exception {

		return this.parent.readRecord(this, guid);
	}

	@Override
	public byte[] readRecordTail(final RecordBdbj record) throws Exception {

		return this.parent.readRecordTail(this, record);
	}

	@Override
	public void reset() {

		throw new UnsupportedOperationException();
	}

	@Override
	public int searchBetween(final Function<Long, ?> target, final Guid key, final Guid value1, final Guid value2, final int limit) throws Exception {

		return this.parent.searchBetween(this, target, key, value1, value2, limit);

	}

	@Override
	public int searchEquals(final Collection<Long> target, final Guid key, final Guid value, final int limit) throws Exception {

		return this.parent.searchEquals(this, target, key, value, limit);
	}

	@Override
	public String toString() {

		return this.getClass().getSimpleName() + " txn=" + this.txn;
	}
}
