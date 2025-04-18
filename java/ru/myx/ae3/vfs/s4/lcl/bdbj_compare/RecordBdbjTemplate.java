package ru.myx.ae3.vfs.s4.lcl.bdbj_compare;

import ru.myx.ae3.vfs.s4.common.RecTemplate;

class RecordBdbjTemplate extends RecordBdbj implements RecTemplate {
	
	private Object	attachment;
	
	@Override
	public Object getAttachment() {
		return this.attachment;
	}
	
	@Override
	public void setAttachment(final Object attachment) {
		this.attachment = attachment;
	}
	
}
