package ru.myx.ae3.vfs.s4.lcl.bdbj;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.Put;

import ru.myx.ae3.base.BaseHostEmpty;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.know.Guid;
import ru.myx.ae3.reflect.Reflect;
import ru.myx.ae3.reflect.ReflectionEnumerable;
import ru.myx.ae3.reflect.ReflectionExplicit;
import ru.myx.ae3.reflect.ReflectionManual;

@ReflectionManual
class RowItem //
		extends
			BaseHostEmpty {

	private static BaseObject PROTOTYPE = Reflect.classToBasePrototype(RowItem.class);

	static boolean findRowItemByKey(final RowItem recordItem, final Cursor cursorItem, final DatabaseEntry key, final DatabaseEntry value, final long key0_luid6) {

		final byte[] keyCache = key.getData();
		final byte[] keyBytes = keyCache == null || keyCache.length < 6
			? new byte[6]
			: keyCache;

		WorkerBdbj.writeLongAsLow6(keyBytes, 0, key0_luid6);
		key.setData(keyBytes, 0, 6);

		if (null != cursorItem.get(key, value, Get.SEARCH, null)) {
			recordItem.key0_luid6 = key0_luid6;
			RowItem.materializeRowItemValue(recordItem, value.getData());
			// RowItem.materializeRowItem( recordItem, keyBytes, value.getData() );
			return true;
		}

		return false;
	}

	static final RowItem materializeRowItem(final RowItem result, final byte[] key, final byte[] value) {

		result.key0_luid6 = WorkerBdbj.readLow6AsLong(key, 0);
		result.val0_schedule2 = (short) (((value[0] & 0xFF) << 8) + (value[1] & 0xFF));
		result.val1_guidX = Guid.readGuid(value, 2);
		/** <code>
		if (key.length != 6) {
			throw new BadKeyException( key, "Key length is wrong: " + key.length + " instead of 6" );
		}
		</code> */
		return result;
	}

	static final RowItem materializeRowItemValue(final RowItem result, final byte[] value) {

		result.val0_schedule2 = (short) (((value[0] & 0xFF) << 8) + (value[1] & 0xFF));
		result.val1_guidX = Guid.readGuid(value, 2);
		return result;
	}

	static void serializeRowItem(final Cursor cursorItem,
			final byte[] byteBuffer,
			final DatabaseEntry key,
			final DatabaseEntry value,
			final long key0_luid6,
			final short val0_schedule2,
			final Guid val1_guidX) {

		WorkerBdbj.writeLongAsLow6(byteBuffer, 0, key0_luid6);
		WorkerBdbj.writeShort(byteBuffer, 6, val0_schedule2);
		final int guidLength = Guid.writeGuid(val1_guidX, byteBuffer, 6 + 2);

		key.setData(byteBuffer, 0, 6);
		value.setData(byteBuffer, 6, 2 + guidLength);

		cursorItem.put(key, value, Put.OVERWRITE, null);
	}

	static int setupItemKey(final byte[] byteBuffer, final int offset, final long key0_luid6) {

		WorkerBdbj.writeLongAsLow6(byteBuffer, offset, key0_luid6);
		return 6;
	}

	static void setupItemKey(final DatabaseEntry key, final byte[] byteBuffer, final long key0_luid6) {

		key.setData(byteBuffer, 0, RowItem.setupItemKey(byteBuffer, 0, key0_luid6));
	}

	@ReflectionExplicit
	@ReflectionEnumerable
	public long key0_luid6;

	@ReflectionExplicit
	@ReflectionEnumerable
	public short val0_schedule2;

	@ReflectionExplicit
	@ReflectionEnumerable
	public Guid val1_guidX;

	@Override
	public BaseObject basePrototype() {

		return RowItem.PROTOTYPE;
	}
}
