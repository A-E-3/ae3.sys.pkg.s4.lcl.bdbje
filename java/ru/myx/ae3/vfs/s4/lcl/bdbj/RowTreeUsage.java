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
class RowTreeUsage //
		extends
			BaseHostEmpty {

	private static BaseObject PROTOTYPE = Reflect.classToBasePrototype(RowTreeUsage.class);

	static boolean findRowTreeUsageByKey(final RowTreeUsage recordTreeUsage,
			final Cursor cursorTreeUsage,
			final DatabaseEntry key,
			final Guid key0_targetX,
			final long key1_luid6,
			final Guid key2_keyX) {

		final int lengthTargetX = Guid.writeGuidByteCount(key0_targetX);
		final int lengthKeyX = Guid.writeGuidByteCount(key2_keyX);
		final byte[] keyCache = key.getData();
		final byte[] keyBytes = keyCache == null || keyCache.length < lengthTargetX + 6 + lengthKeyX
			? new byte[lengthKeyX + lengthTargetX + 6]
			: keyCache;
		RowTreeUsage.setupTreeUsageKey(key, keyBytes, key0_targetX, key1_luid6, key2_keyX);

		if (null != cursorTreeUsage.get(key, null, Get.SEARCH, null)) {
			recordTreeUsage.key0_targetX = key0_targetX;
			recordTreeUsage.key1_luid6 = key1_luid6;
			recordTreeUsage.key2_keyX = key2_keyX;
			// RowTreeUsage.materializeRowTreeUsage( recordTreeUsage, keyBytes );
			return true;
		}

		return false;
	}

	static RowTreeUsage materializeRowTreeUsage(final RowTreeUsage result, final byte[] key) {

		try {
			final int lengthTargetX = Guid.writeGuidByteCount(result.key0_targetX = Guid.readGuid(key, 0));
			result.key1_luid6 = WorkerBdbj.readLow6AsLong(key, lengthTargetX);
			result.key2_keyX = Guid.readGuid(key, lengthTargetX + 6);
			return result;
		} catch (IndexOutOfBoundsException | IllegalArgumentException e) {
			throw new BadKeyException(key, e);
		}
	}

	static void serializeRowTreeUsage(final Cursor cursorTreeUsage,
			final byte[] byteBuffer,
			final DatabaseEntry key,
			final DatabaseEntry value,
			final Guid key0_targetX,
			final long key1_luid6,
			final Guid key2_keyX) {

		RowTreeUsage.setupTreeUsageKey(key, byteBuffer, key0_targetX, key1_luid6, key2_keyX);
		value.setData(byteBuffer, 0, 0);
		cursorTreeUsage.put(key, value, Put.OVERWRITE, null);
	}

	static int setupTreeUsageKey(final byte[] byteBuffer, final int offset, final Guid key0_targetX, final long key1_luid6, final Guid key2_keyX) {

		final int lengthTargetX = Guid.writeGuidByteCount(key0_targetX);
		final int lengthKeyX = Guid.writeGuidByteCount(key2_keyX);
		Guid.writeGuid(key0_targetX, byteBuffer, offset);
		WorkerBdbj.writeLongAsLow6(byteBuffer, offset + lengthTargetX, key1_luid6);
		Guid.writeGuid(key2_keyX, byteBuffer, offset + lengthTargetX + 6);
		return lengthTargetX + 6 + lengthKeyX;
	}

	static void setupTreeUsageKey(final DatabaseEntry key, final byte[] byteBuffer, final Guid key0_targetX, final long key1_luid6, final Guid key2_keyX) {

		key.setData(byteBuffer, 0, RowTreeUsage.setupTreeUsageKey(byteBuffer, 0, key0_targetX, key1_luid6, key2_keyX));
	}

	@ReflectionExplicit
	@ReflectionEnumerable
	public Guid key0_targetX;

	@ReflectionExplicit
	@ReflectionEnumerable
	public long key1_luid6;

	@ReflectionExplicit
	@ReflectionEnumerable
	public Guid key2_keyX;

	@Override
	public BaseObject basePrototype() {

		return RowTreeUsage.PROTOTYPE;
	}
}
