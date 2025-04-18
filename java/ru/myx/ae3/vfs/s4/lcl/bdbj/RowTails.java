package ru.myx.ae3.vfs.s4.lcl.bdbj;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;

import ru.myx.ae3.base.BaseHostEmpty;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.binary.Transfer;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.know.Guid;
import ru.myx.ae3.reflect.Reflect;
import ru.myx.ae3.reflect.ReflectionEnumerable;
import ru.myx.ae3.reflect.ReflectionExplicit;
import ru.myx.ae3.reflect.ReflectionManual;
import ru.myx.ae3.vfs.s4.driver.S4Driver;

/** TODO: PROPOSAL!!!
 *
 * key:azimuth4;key:guidX;val:schedule2;val:luid6;val:dataX
 *
 * @author myx */
@ReflectionManual
final class RowTails //
		extends
			BaseHostEmpty {

	private static final BaseObject PROTOTYPE = Reflect.classToBasePrototype(RowTails.class);

	static final boolean findRowTailsByKey(//
			final RowTails recordTails,
			final Cursor cursorTails,
			final DatabaseEntry key,
			final DatabaseEntry value,
			final int key0_azimuth4,
			final Guid key1_guidX//
	) {

		final byte[] keyCache = key.getData();
		final byte[] keyBytes = keyCache == null || keyCache.length < 6
			? new byte[6]
			: keyCache;
		RowTails.setupTailsKey(key, keyBytes, key0_azimuth4, key1_guidX);

		if (null != cursorTails.get(key, value, Get.SEARCH, null)) {
			recordTails.key0_azimuth4 = key0_azimuth4;
			recordTails.key1_guidX = key1_guidX;
			RowTails.materializeRowTailValue(recordTails, value.getData());
			/** can't check size!!! **/
			// RowTail.materializeRowTail(recordTail, key.getData(), value.getData());
			return true;
		}

		return false;
	}

	static final boolean guidIsBinaryTail(final Guid guid) {

		return guid.isBinary() && !guid.isInline() && guid.getBinaryLength() <= S4Driver.TAIL_CAPACITY;
	}

	static final RowTails materializeRowTail(final RowTails result, final byte[] key, final byte[] value) {

		if (key.length < 5) {
			throw new BadKeyException(key, "Key length is wrong: " + key.length + " < 5");
		}
		result.key0_azimuth4 = WorkerBdbj.readInt(key, 0);
		result.key1_guidX = Guid.readGuid(key, 4);
		result.val0_schedule2 = (short) (((value[0] & 0xFF) << 8) + (value[1] & 0xFF));
		result.val1_luid6 = WorkerBdbj.readLow6AsLong(value, 2);
		System.arraycopy(value, 4, result.val2_dataX = new byte[value.length - (2 + 6)], 0, value.length - (2 + 6));
		// result.val2_dataX = Transfer.wrapCopier(value, 2 + 6, value.length - (2 + 6));
		return result;
	}

	static final RowTails materializeRowTailValue(final RowTails result, final byte[] value) {

		if (value.length < 8) {
			throw new BadKeyException(value, "Value length is wrong: " + value.length + " < 8");
		}
		System.arraycopy(value, 4, result.val2_dataX = new byte[value.length - (2 + 6)], 0, value.length - (2 + 6));
		// result.val2_dataX = Transfer.wrapCopier(value, 2 + 6, value.length - (2 + 6));
		return result;
	}

	static final int setupTailsKey(final byte[] byteBuffer, final int offset, final int key0_azimuth4, final Guid key1_guidX) {

		assert key0_azimuth4 == key1_guidX.hashCode();

		WorkerBdbj.writeInt(byteBuffer, offset, key0_azimuth4);
		final int lengthKeyX = Guid.writeGuid(key1_guidX, byteBuffer, offset + 4);
		return 4 + lengthKeyX;
	}

	static final void setupTailsKey(final DatabaseEntry key, final byte[] byteBuffer, final int key0_azimuth4, final Guid key1_guidX) {

		key.setData(byteBuffer, 0, RowTails.setupTailsKey(byteBuffer, 0, key0_azimuth4, key1_guidX));
	}

	@ReflectionExplicit
	@ReflectionEnumerable
	public long key0_azimuth4;

	@ReflectionExplicit
	@ReflectionEnumerable
	public Guid key1_guidX;

	@ReflectionExplicit
	@ReflectionEnumerable
	public short val0_schedule2;

	@ReflectionExplicit
	@ReflectionEnumerable
	public long val1_luid6;

	@ReflectionExplicit
	@ReflectionEnumerable
	public byte[] val2_dataX;
	// public TransferCopier val2_dataX;

	@Override
	public BaseObject basePrototype() {

		return RowTails.PROTOTYPE;
	}

	@ReflectionExplicit
	public TransferCopier getVal2_dataX() {

		return Transfer.wrapCopier(this.val2_dataX);
		// return this.val2_dataX;
	}

	@ReflectionExplicit
	public void setVal2_dataX(final TransferCopier data) {

		this.val2_dataX = data.nextDirectArray();
		// this.val2_dataX = data;
	}
}
