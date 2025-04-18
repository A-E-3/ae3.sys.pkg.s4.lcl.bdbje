package ru.myx.ae3.vfs.s4.lcl.bdbj;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.Transaction;

class XctBdbjTxnGlobal //
		extends
			XctBdbjSimple {
	
	static final DatabaseEntry DUMMY_KEY;
	
	static {
		DUMMY_KEY = new DatabaseEntry(new byte[]{
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
				(byte) 255, //
		});
	}
	
	XctBdbjTxnGlobal(final WorkerBdbj parent, final Transaction txn) {
		
		super(parent, txn, CursorConfig.READ_UNCOMMITTED);
	}
	
	@Override
	public void cancel() throws Exception {
		
		throw new UnsupportedOperationException("No 'cancel' on implicit transaction!");
	}
	
	@Override
	public void close() {
		
		//
	}
	
	@Override
	public void closeImpl() {
		
		super.closeImpl();
		if (this.txn != null) {
			this.txn.abort();
		}
	}
	
	@Override
	public void commit() throws Exception {
		
		throw new UnsupportedOperationException("No 'commit' on implicit transaction!");
	}
	
	@Override
	public Cursor getCursorItem(final WorkerBdbj local) {
		
		if (this.cursorItem != null) {
			return this.cursorItem;
		}
		return this.cursorItem = super.getCursorItem(local);
	}
	
	@Override
	public Cursor getCursorItemGuid(final WorkerBdbj local) {
		
		if (this.cursorItemGuid != null) {
			return this.cursorItemGuid;
		}
		return this.cursorItemGuid = super.getCursorItemGuid(local);
	}
	
	@Override
	public Cursor getCursorItemQueue(final WorkerBdbj local) {
		
		if (this.cursorItemQueue != null) {
			return this.cursorItemQueue;
		}
		return this.cursorItemQueue = super.getCursorItemQueue(local);
	}
	
	@Override
	public Cursor getCursorTail(final WorkerBdbj local) {
		
		if (this.cursorTail != null) {
			return this.cursorTail;
		}
		return this.cursorTail = super.getCursorTail(local);
	}
	
	@Override
	public Cursor getCursorTree(final WorkerBdbj local) {
		
		if (this.cursorTree != null) {
			return this.cursorTree;
		}
		return this.cursorTree = super.getCursorTree(local);
	}
	
	@Override
	public Cursor getCursorTreeIndex(final WorkerBdbj local) {
		
		if (this.cursorTreeIndex != null) {
			return this.cursorTreeIndex;
		}
		return this.cursorTreeIndex = super.getCursorTreeIndex(local);
	}
	
	@Override
	public Cursor getCursorTreeUsage(final WorkerBdbj local) {
		
		if (this.cursorTreeUsage != null) {
			return this.cursorTreeUsage;
		}
		return this.cursorTreeUsage = super.getCursorTreeUsage(local);
	}
	
	@Override
	public void reset() {
		
		if (this.cursorTree != null) {
			this.cursorTree.get(XctBdbjTxnGlobal.DUMMY_KEY, null, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN);
		}
		if (this.cursorTreeIndex != null) {
			this.cursorTreeIndex.get(XctBdbjTxnGlobal.DUMMY_KEY, null, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN);
		}
		if (this.cursorTreeUsage != null) {
			this.cursorTreeUsage.get(XctBdbjTxnGlobal.DUMMY_KEY, null, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN);
		}
		if (this.cursorItem != null) {
			this.cursorItem.get(XctBdbjTxnGlobal.DUMMY_KEY, null, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN);
		}
		if (this.cursorItemGuid != null) {
			this.cursorItemGuid.get(XctBdbjTxnGlobal.DUMMY_KEY, null, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN);
		}
		if (this.cursorItemQueue != null) {
			this.cursorItemQueue.get(XctBdbjTxnGlobal.DUMMY_KEY, null, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN);
		}
		if (this.cursorTail != null) {
			this.cursorTail.get(XctBdbjTxnGlobal.DUMMY_KEY, null, Get.SEARCH, WorkerBdbj.RO_FOR_SCAN);
		}
	}
}
