package ru.myx.ae3.vfs.s4.lcl.bdbj;

/** @author myx */
final class BadKeyException //
		extends
			IllegalArgumentException {

	private static final long serialVersionUID = 154413231740618558L;

	final byte[] key;

	BadKeyException(final byte[] key, final String message) {

		super(message);
		this.key = key;
	}

	BadKeyException(final byte[] key, final Throwable cause) {

		super(cause);
		this.key = key;
	}

	@Override
	public String getMessage() {

		final Throwable cause = this.getCause();
		return cause == null
			? super.getMessage()
			: cause.getMessage();
	}
}
