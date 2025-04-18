package ru.myx.ae3.vfs.s4.lcl.bdbj;

import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

import ru.myx.ae3.Engine;
import ru.myx.ae3.help.Format;

class XctBdbjTxnTempGlobal //
		extends
			XctBdbjSimple {

	private final static TransactionConfig TXN_CONFIG;
	static {
		TXN_CONFIG = new TransactionConfig();
		XctBdbjTxnTempGlobal.TXN_CONFIG.setReadCommitted(false);
		XctBdbjTxnTempGlobal.TXN_CONFIG.setDurability(Durability.READ_ONLY_TXN);
	}

	private final Environment environment;

	private long timestamp;

	Transaction txnPrevious;

	XctBdbjTxnTempGlobal(final WorkerBdbj parent, final Environment environment) {

		super(parent, environment.beginTransaction(null, XctBdbjTxnTempGlobal.TXN_CONFIG), CursorConfig.READ_UNCOMMITTED);
		this.environment = environment;
		this.timestamp = Engine.fastTime();
	}

	@Override
	public void cancel() throws Exception {

		throw new UnsupportedOperationException("No 'cancel' on implicit transaction!");
	}

	@Override
	public void close() {

		super.closeImpl();
		if (Engine.fastTime() - this.timestamp > 7_000L) {
			Transaction previous = null;
			synchronized (this) {
				if (Engine.fastTime() - this.timestamp > 7_000L) {
					previous = this.txnPrevious;
					this.txnPrevious = this.txn;
					this.txn = this.environment.beginTransaction(null, XctBdbjTxnTempGlobal.TXN_CONFIG);
					this.timestamp = Engine.fastTime();
				}
			}
			if (previous != null) {
				previous.abort();
			}
		}
	}

	@Override
	public void commit() throws Exception {

		throw new UnsupportedOperationException("No 'commit' on implicit transaction!");
	}

	@Override
	public void reset() {

		this.close();
	}

	@Override
	public String toString() {

		return this.getClass().getSimpleName() + " txn=" + this.txn + ", age=" + Format.Compact.toPeriod(Engine.fastTime() - this.timestamp);
	}
}
