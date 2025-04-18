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
class RowTreeIndex //
		extends
			BaseHostEmpty {
	
	private static BaseObject PROTOTYPE = Reflect.classToBasePrototype(RowTreeIndex.class);
	
	static boolean findRowTreeIndexByKey(final RowTreeIndex recordTreeIndex,
			final Cursor cursorTreeIndex,
			final DatabaseEntry key,
			final Guid key0_keyX,
			final Guid key1_targetX,
			final long key2_luid6) throws IndexOutOfBoundsException, IllegalArgumentException {
		
		final int lengthKeyX = Guid.writeGuidByteCount(key0_keyX);
		final int lengthTargetX = Guid.writeGuidByteCount(key1_targetX);
		final byte[] keyCache = key.getData();
		final byte[] keyBytes = keyCache == null || keyCache.length < lengthTargetX + 6 + lengthKeyX
			? new byte[lengthKeyX + lengthTargetX + 6]
			: keyCache;
		RowTreeIndex.setupTreeIndexKey(key, keyBytes, key0_keyX, key1_targetX, key2_luid6);
		
		if (null != cursorTreeIndex.get(key, null, Get.SEARCH, null)) {
			recordTreeIndex.key0_keyX = key0_keyX;
			recordTreeIndex.key1_targetX = key1_targetX;
			recordTreeIndex.key2_luid6 = key2_luid6;
			// RowTreeIndex.materializeRowTreeIndex( recordTreeIndex, keyBytes );
			return true;
		}
		
		return false;
	}
	
	static RowTreeIndex materializeRowTreeIndex(final RowTreeIndex result, final byte[] key) throws IndexOutOfBoundsException, IllegalArgumentException {
		
		try {
			final int lengthKeyX = Guid.writeGuidByteCount(result.key0_keyX = Guid.readGuid(key, 0));
			final int lengthTargetX = Guid.writeGuidByteCount(result.key1_targetX = Guid.readGuid(key, lengthKeyX));
			result.key2_luid6 = WorkerBdbj.readLow6AsLong(key, lengthKeyX + lengthTargetX);
			return result;
		} catch (IndexOutOfBoundsException | IllegalArgumentException e) {
			throw new BadKeyException(key, e);
		}
	}
	
	static void serializeRowTreeIndex(final Cursor cursorTreeIndex,
			final byte[] byteBuffer,
			final DatabaseEntry key,
			final DatabaseEntry value,
			final Guid key0_keyX,
			final Guid key1_targetX,
			final long key2_luid6) {
		
		RowTreeIndex.setupTreeIndexKey(key, byteBuffer, key0_keyX, key1_targetX, key2_luid6);
		value.setData(byteBuffer, 0, 0);
		cursorTreeIndex.put(key, value, Put.OVERWRITE, null);
	}
	
	static int setupTreeIndexKey(final byte[] byteBuffer, final int offset, final Guid key0_keyX, final Guid key1_targetX, final long key2_luid6) {
		
		final int lengthKeyX = Guid.writeGuid(key0_keyX, byteBuffer, offset);
		final int lengthTargetX = Guid.writeGuid(key1_targetX, byteBuffer, offset + lengthKeyX);
		WorkerBdbj.writeLongAsLow6(byteBuffer, offset + lengthTargetX + lengthKeyX, key2_luid6);
		return lengthKeyX + lengthTargetX + 6;
	}
	
	static void setupTreeIndexKey(final DatabaseEntry key, final byte[] byteBuffer, final Guid key0_keyX, final Guid key1_targetX, final long key2_luid6) {
		
		key.setData(byteBuffer, 0, RowTreeIndex.setupTreeIndexKey(byteBuffer, 0, key0_keyX, key1_targetX, key2_luid6));
	}
	
	@ReflectionExplicit
	@ReflectionEnumerable
	public Guid key0_keyX;
	
	@ReflectionExplicit
	@ReflectionEnumerable
	public Guid key1_targetX;
	
	@ReflectionExplicit
	@ReflectionEnumerable
	public long key2_luid6;
	
	@Override
	public BaseObject basePrototype() {
		
		return RowTreeIndex.PROTOTYPE;
	}
	
}
