package ru.myx.ae3.vfs.s4.lcl.bdbj;

import com.sleepycat.je.Cursor;

import ru.myx.ae3.vfs.s4.driver.S4WorkerTransaction;

abstract class XctBdbj //
		implements
			S4WorkerTransaction<RecordBdbj, ReferenceBdbj, Long>,
			AutoCloseable {

	@Override
	public abstract void cancel() throws Exception;

	@Override
	public void close() throws Exception {

		this.cancel();
	}

	@Override
	public abstract void commit() throws Exception;

	/** to actually destroy resource allocation */
	abstract void destroy();

	/** luid6
	 *
	 * ->
	 *
	 * schedule2;guidX
	 *
	 * @param worker
	 * @return */
	public abstract Cursor getCursorItem(final WorkerBdbj worker);

	/** azimuth4;luid6
	 *
	 * ->
	 *
	 * schedule2;guidX
	 *
	 * @param worker
	 * @return */
	public abstract Cursor getCursorItemGuid(final WorkerBdbj worker);

	/** queue:
	 *
	 * schedule2;luid6
	 *
	 * ->
	 *
	 * ()
	 *
	 * @param worker
	 * @return */
	public abstract Cursor getCursorItemQueue(final WorkerBdbj worker);

	/** ???:
	 *
	 * luid6
	 *
	 * ->
	 *
	 * dataX
	 *
	 *
	 * Cleaned by life-cycle garbage collector.
	 *
	 * @param worker
	 * @return */
	public abstract Cursor getCursorTail(final WorkerBdbj worker);

	/** tree:
	 *
	 * luid6;keyX
	 *
	 * ->
	 *
	 * mode1;modified6;targetX
	 *
	 * @param worker
	 * @return */
	public abstract Cursor getCursorTree(final WorkerBdbj worker);

	/** ???:
	 *
	 * keyX;targetX;luid6
	 *
	 * ->
	 *
	 * ()
	 *
	 * @param worker
	 * @return */
	public abstract Cursor getCursorTreeIndex(final WorkerBdbj worker);

	/** ???:
	 *
	 * targetX;luid6;keyX
	 *
	 * ->
	 *
	 * ()
	 *
	 * @param worker
	 * @return */
	public abstract Cursor getCursorTreeUsage(final WorkerBdbj worker);

	public void reset() throws Exception {

		//
	}

	@Override
	public abstract String toString();
}
