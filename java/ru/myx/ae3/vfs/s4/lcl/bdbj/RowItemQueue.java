package ru.myx.ae3.vfs.s4.lcl.bdbj;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.Put;

import ru.myx.ae3.base.BaseHostEmpty;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.reflect.Reflect;
import ru.myx.ae3.reflect.ReflectionEnumerable;
import ru.myx.ae3.reflect.ReflectionExplicit;
import ru.myx.ae3.reflect.ReflectionManual;

@ReflectionManual
class RowItemQueue //
		extends
			BaseHostEmpty {

	private static BaseObject PROTOTYPE = Reflect.classToBasePrototype(RowItemQueue.class);

	static boolean findRowItemQueueByKey(//
			final RowItemQueue recordQueue,
			final Cursor cursorItemQueue,
			final DatabaseEntry key,
			final short key0_schedule2,
			final long key1_luid6//
	) {

		final byte[] keyCache = key.getData();
		final byte[] keyBytes = keyCache == null || keyCache.length < 6 + 2
			? new byte[6 + 2]
			: keyCache;
		RowItemQueue.setupItemQueueKey(key, keyBytes, key0_schedule2, key1_luid6);

		if (null != cursorItemQueue.get(key, null, Get.SEARCH, null)) {
			recordQueue.key0_schedule2 = key0_schedule2;
			recordQueue.key1_luid6 = key1_luid6;
			/** can't check size!!! **/
			// RowItemQueue.materializeRowItemQueue( recordQueue, keyBytes );
			return true;
		}

		return false;
	}

	static final RowItemQueue materializeRowItemQueue(//
			final RowItemQueue result,
			final byte[] key//
	) {

		if (key.length != 2 + 6) {
			throw new BadKeyException(key, "Key length is wrong: " + key.length + " instead of 8");
		}
		result.key0_schedule2 = (short) (((key[0] & 0xFF) << 8) + (key[1] & 0xFF));
		result.key1_luid6 = WorkerBdbj.readLow6AsLong(key, 2);
		return result;
	}

	static void serializeRowItemQueue(final Cursor cursorItemQueue,
			final byte[] byteBuffer,
			final DatabaseEntry key,
			final DatabaseEntry value,
			final short key0_schedule2,
			final long key1_luid6) {

		RowItemQueue.setupItemQueueKey(key, byteBuffer, key0_schedule2, key1_luid6);
		value.setData(byteBuffer, 0, 0);
		cursorItemQueue.put(key, value, Put.OVERWRITE, null);
	}

	static int setupItemQueueKey(//
			final byte[] byteBuffer,
			final int offset,
			final short key0_schedule2,
			final long key1_luid6//
	) {

		WorkerBdbj.writeShort(byteBuffer, offset + 0, key0_schedule2);
		WorkerBdbj.writeLongAsLow6(byteBuffer, offset + 2, key1_luid6);
		return 2 + 6;
	}

	static void setupItemQueueKey(//
			final DatabaseEntry key,
			final byte[] byteBuffer,
			final short key0_schedule2,
			final long key1_luid6//
	) {

		key.setData(byteBuffer, 0, RowItemQueue.setupItemQueueKey(byteBuffer, 0, key0_schedule2, key1_luid6));
	}

	@ReflectionExplicit
	@ReflectionEnumerable
	public short key0_schedule2;

	@ReflectionExplicit
	@ReflectionEnumerable
	public long key1_luid6;

	@Override
	public BaseObject basePrototype() {

		return RowItemQueue.PROTOTYPE;
	}
}
