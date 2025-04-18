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

/** key:luid6;val:dataX
 *
 * @author myx */
@ReflectionManual
final class RowTail //
		extends
			BaseHostEmpty {
	
	private static final BaseObject PROTOTYPE = Reflect.classToBasePrototype(RowTail.class);
	
	static final boolean findRowTailByKey(//
			final RowTail recordTail,
			final Cursor cursorTail,
			final DatabaseEntry key,
			final DatabaseEntry value,
			final long luid6//
	) {
		
		final byte[] keyCache = key.getData();
		final byte[] keyBytes = keyCache == null || keyCache.length < 6
			? new byte[6]
			: keyCache;
		RowTail.setupTailKey(key, keyBytes, luid6);
		
		if (null != cursorTail.get(key, value, Get.SEARCH, null)) {
			recordTail.key0_luid6 = luid6;
			recordTail.val0_dataX = value.getData();
			/** can't check size!!! **/
			// RowTail.materializeRowTail(recordTail, key.getData(), value.getData());
			return true;
		}
		
		return false;
	}
	
	static final boolean guidIsBinaryTail(final Guid guid) {
		
		return guid.isBinary() && !guid.isInline() && guid.getBinaryLength() <= S4Driver.TAIL_CAPACITY;
	}
	
	static final RowTail materializeRowTail(final RowTail result, final byte[] key, final byte[] value) {
		
		if (key.length != 6) {
			throw new BadKeyException(key, "Key length is wrong: " + key.length + " instead of 6");
		}
		result.key0_luid6 = WorkerBdbj.readLow6AsLong(key, 0);
		result.val0_dataX = value;
		return result;
	}
	
	static final RowTail materializeRowTailValue(final RowTail result, final byte[] value) {
		
		result.val0_dataX = value;
		return result;
	}
	
	static final int setupTailKey(final byte[] byteBuffer, final int offset, final long key0_luid6) {
		
		WorkerBdbj.writeLongAsLow6(byteBuffer, offset, key0_luid6);
		return 6;
	}
	
	static final void setupTailKey(final DatabaseEntry key, final byte[] byteBuffer, final long key0_luid6) {
		
		key.setData(byteBuffer, 0, RowTail.setupTailKey(byteBuffer, 0, key0_luid6));
	}
	
	@ReflectionExplicit
	@ReflectionEnumerable
	public long key0_luid6;
	
	@ReflectionExplicit
	@ReflectionEnumerable
	public byte[] val0_dataX;
	
	@Override
	public BaseObject basePrototype() {
		
		return RowTail.PROTOTYPE;
	}
	
	@ReflectionExplicit
	public TransferCopier getVal0_dataX() {
		
		return Transfer.wrapCopier(this.val0_dataX);
	}
	
	@ReflectionExplicit
	public void setVal0_dataX(final TransferCopier data) {
		
		this.val0_dataX = data.nextDirectArray();
	}
}
