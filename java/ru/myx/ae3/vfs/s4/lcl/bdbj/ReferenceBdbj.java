package ru.myx.ae3.vfs.s4.lcl.bdbj;

import ru.myx.ae3.know.Guid;
import ru.myx.ae3.vfs.TreeLinkType;
import ru.myx.ae3.vfs.s4.common.RefImpl;

final class ReferenceBdbj //
		extends
			RefImpl<RecordBdbj> {

	ReferenceBdbj() {

		// ignore
	}

	ReferenceBdbj(final Guid key, final TreeLinkType mode, final Guid target) {

		this.key = key;
		this.mode = mode;
		this.value = target;
	}
}
