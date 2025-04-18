package ru.myx.ae3.vfs.s4.lcl.bdbj_compare;

import ru.myx.ae3.vfs.s4.common.RecImpl;

class RecordBdbj extends RecImpl {
	long	luid;
	
	@Override
	public String toString() {
		return "guid=" + this.guid + "&luid=" + this.luid + "&schedule=" + this.scheduleBits;
	}
}
