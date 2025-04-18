package ru.myx.ae3.vfs.s4.lcl.bdbj;

import com.sleepycat.je.CursorConfig;

class XctBdbjNoTxnGlobal //
		extends
			XctBdbjSimple {
	
	XctBdbjNoTxnGlobal(final WorkerBdbj parent) {
		
		super(parent, null, CursorConfig.READ_UNCOMMITTED);
	}
	
	@Override
	public void cancel() throws Exception {
		
		//
	}
	
	@Override
	public void commit() throws Exception {
		
		//
	}
	
	@Override
	public void reset() {
		
		//
	}
}
