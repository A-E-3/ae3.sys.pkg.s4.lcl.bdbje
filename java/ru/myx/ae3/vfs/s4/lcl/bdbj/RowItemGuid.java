package ru.myx.ae3.vfs.s4.lcl.bdbj;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;
import com.sleepycat.je.OperationResult;
import com.sleepycat.je.Put;

import ru.myx.ae3.base.BaseHostEmpty;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.know.Guid;
import ru.myx.ae3.reflect.Reflect;
import ru.myx.ae3.reflect.ReflectionEnumerable;
import ru.myx.ae3.reflect.ReflectionExplicit;
import ru.myx.ae3.reflect.ReflectionManual;

@ReflectionManual
class RowItemGuid //
		extends
			BaseHostEmpty {
	
	private static BaseObject PROTOTYPE = Reflect.classToBasePrototype(RowItemGuid.class);
	
	static boolean findRowItemGuidByGuid(final RowItemGuid recordGuid,
			final Cursor cursorItemGuid,
			final byte[] byteBuffer,
			final DatabaseEntry key,
			final DatabaseEntry value,
			final Guid guidX) {
		
		final int azimuth = guidX.hashCode();
		WorkerBdbj.writeInt(byteBuffer, 0, azimuth);
		key.setData(byteBuffer, 0, 4);
		
		for (OperationResult result = cursorItemGuid.get(key, value, Get.SEARCH_GTE, null);;) {
			if (result == null) {
				return false;
			}
			RowItemGuid.materializeRowItemGuid(recordGuid, key.getData(), value.getData());
			if (azimuth != recordGuid.key0_azimuth4) {
				return false;
			}
			if (guidX.compareTo(recordGuid.val1_guidX) == 0) {
				return true;
			}
			result = cursorItemGuid.get(key, value, Get.NEXT, null);
		}
	}
	
	static boolean findRowItemGuidByKey(final RowItemGuid recordGuid,
			final Cursor cursorItemGuid,
			final DatabaseEntry key,
			final DatabaseEntry value,
			final int key0_azimuth,
			final long key1_luid6) {
		
		final byte[] keyCache = key.getData();
		final byte[] keyBytes = keyCache == null || keyCache.length < 4 + 6
			? new byte[4 + 6]
			: keyCache;
		
		WorkerBdbj.writeInt(keyBytes, 0, key0_azimuth);
		WorkerBdbj.writeLongAsLow6(keyBytes, 4, key1_luid6);
		key.setData(keyBytes, 0, 4 + 6);
		
		if (null != cursorItemGuid.get(key, value, Get.SEARCH, null)) {
			recordGuid.key0_azimuth4 = key0_azimuth;
			recordGuid.key1_luid6 = key1_luid6;
			RowItemGuid.materializeRowItemGuidValue(recordGuid, value.getData());
			/** can't check size!!! **/
			// RowItemGuid.materializeRowItemGuid( recordGuid, keyBytes,
			// value.getData() );
			return true;
		}
		
		return false;
	}
	
	static final RowItemGuid materializeRowItemGuid(//
			final RowItemGuid result,
			final byte[] key,
			final byte[] value//
	) {
		
		if (key.length != 4 + 6) {
			throw new BadKeyException(key, "Key length is wrong: " + key.length + " instead of 10");
		}
		result.key0_azimuth4 = WorkerBdbj.readInt(key, 0);
		result.key1_luid6 = WorkerBdbj.readLow6AsLong(key, 4);
		result.val0_schedule2 = (short) (((value[0] & 0xFF) << 8) + (value[1] & 0xFF));
		result.val1_guidX = Guid.readGuid(value, 2);
		return result;
	}
	
	static final RowItemGuid materializeRowItemGuidValue(final RowItemGuid result, final byte[] value) {
		
		result.val0_schedule2 = (short) (((value[0] & 0xFF) << 8) + (value[1] & 0xFF));
		result.val1_guidX = Guid.readGuid(value, 2);
		return result;
	}
	
	static void serializeRowItemGuid(final Cursor cursorItemGuid,
			final byte[] byteBuffer,
			final DatabaseEntry key,
			final DatabaseEntry value,
			final int key0_azimuth4,
			final long key1_luid6,
			final short val0_schedule2,
			final Guid val1_guidX) {
		
		WorkerBdbj.writeInt(byteBuffer, 0, key0_azimuth4);
		WorkerBdbj.writeLongAsLow6(byteBuffer, 4, key1_luid6);
		WorkerBdbj.writeShort(byteBuffer, 4 + 6, val0_schedule2);
		final int guidLength = Guid.writeGuid(val1_guidX, byteBuffer, 4 + 6 + 2);
		
		key.setData(byteBuffer, 0, 4 + 6);
		value.setData(byteBuffer, 4 + 6, 2 + guidLength);
		
		cursorItemGuid.put(key, value, Put.OVERWRITE, null);
	}
	
	@ReflectionExplicit
	@ReflectionEnumerable
	public int key0_azimuth4;
	
	@ReflectionExplicit
	@ReflectionEnumerable
	public long key1_luid6;
	
	@ReflectionExplicit
	@ReflectionEnumerable
	public short val0_schedule2;
	
	@ReflectionExplicit
	@ReflectionEnumerable
	public Guid val1_guidX;
	
	@Override
	public BaseObject basePrototype() {
		
		return RowItemGuid.PROTOTYPE;
	}
}
