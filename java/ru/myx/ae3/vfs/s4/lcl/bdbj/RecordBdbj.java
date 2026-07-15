package ru.myx.ae3.vfs.s4.lcl.bdbj;

import ru.myx.ae3.know.Guid;
import ru.myx.ae3.vfs.s4.common.RecImpl;

class RecordBdbj //
		extends
			RecImpl {

	long luid;

	RecordBdbj() {

		// ignore
	}
	
	RecordBdbj(final long luid, final Guid guid, final short runtimeState, final short scheduleBits) {

		super(guid, runtimeState, scheduleBits);
		this.luid = luid;
	}
	
	@Override
	public String toString() {

		return "guid=" + this.guid + "&luid=" + this.luid + "&schedule=" + this.scheduleBits;
	}
}
