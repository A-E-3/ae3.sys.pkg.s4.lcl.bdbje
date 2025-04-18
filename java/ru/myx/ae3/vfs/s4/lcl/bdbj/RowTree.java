package ru.myx.ae3.vfs.s4.lcl.bdbj;

import java.sql.Date;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Get;

import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseDate;
import ru.myx.ae3.base.BaseHostEmpty;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.know.Guid;
import ru.myx.ae3.reflect.Reflect;
import ru.myx.ae3.reflect.ReflectionEnumerable;
import ru.myx.ae3.reflect.ReflectionExplicit;
import ru.myx.ae3.reflect.ReflectionManual;
import ru.myx.ae3.vfs.TreeLinkType;

@ReflectionManual
final class RowTree //
		extends
			BaseHostEmpty {

	private static final BaseObject PROTOTYPE = Reflect.classToBasePrototype(RowTree.class);

	static final boolean findRowTreeByKey(//
			final RowTree recordTree,
			final Cursor cursorTree,
			final DatabaseEntry key,
			final DatabaseEntry value,
			final long key0_luid6,
			final Guid key1_keyX) {

		final int lengthKeyX = Guid.writeGuidByteCount(key1_keyX);
		final byte[] keyCache = key.getData();
		final byte[] keyBytes = keyCache == null || keyCache.length < 6 + lengthKeyX
			? new byte[6 + lengthKeyX]
			: keyCache;
		RowTree.setupTreeKey(key, keyBytes, key0_luid6, key1_keyX);

		if (null != cursorTree.get(key, value, Get.SEARCH, null)) {
			recordTree.key0_luid6 = key0_luid6;
			recordTree.key1_keyX = key1_keyX;
			RowTree.materializeRowTreeValue(recordTree, value.getData());
			// RowTree.materializeRowTree( recordTree, keyBytes, value.getData() );
			return true;
		}

		return false;
	}

	static final boolean guidIsTreeCollection(final Guid guid) {
		
		return guid == Guid.GUID_NULL || guid.isCollection();
	}

	static final RowTree materializeRowTree(final RowTree result, final byte[] key, final byte[] value) {

		try {
			result.key0_luid6 = WorkerBdbj.readLow6AsLong(key, 0);
			result.key1_keyX = Guid.readGuid(key, 6);
			result.val0_mode1 = value[0];
			result.val1_modified6 = WorkerBdbj.readLow6AsLong(value, 1);
			result.val2_targetX = Guid.readGuid(value, 7);
			return result;
		} catch (IndexOutOfBoundsException | IllegalArgumentException e) {
			throw new BadKeyException(key, e);
		}

		/** <code>
		final int keyLength = 6 + Guid.writeGuidByteCount( result.key01_keyX );
		final int valueLength = 1 + 6 + Guid.writeGuidByteCount( result.val02_targetX );
		if (key.length != keyLength) {
			throw new ArrayIndexOutOfBoundsException( "Key length is wrong: " + key.length + " instead of " + keyLength );
		}
		if (value.length != valueLength) {
			throw new ArrayIndexOutOfBoundsException( "Value length is wrong: "
					+ value.length
					+ " instead of "
					+ valueLength );
		}
		return result;
		</code> **/
	}

	static final RowTree materializeRowTreeValue(final RowTree result, final byte[] value) {

		try {
			result.val0_mode1 = value[0];
			result.val1_modified6 = WorkerBdbj.readLow6AsLong(value, 1);
			result.val2_targetX = Guid.readGuid(value, 7);
			return result;
		} catch (IndexOutOfBoundsException | IllegalArgumentException e) {
			throw new BadKeyException(value, e);
		}
	}

	static int setupTreeKey(final byte[] byteBuffer, final int offset, final long key0_luid6, final Guid key1_keyX) {

		WorkerBdbj.writeLongAsLow6(byteBuffer, offset + 0, key0_luid6);
		final int keyLength = Guid.writeGuid(key1_keyX, byteBuffer, 6);
		return 6 + keyLength;
	}

	static final void setupTreeKey(final DatabaseEntry key, final byte[] byteBuffer, final long key0_luid6, final Guid key1_keyX) {

		key.setData(byteBuffer, 0, RowTree.setupTreeKey(byteBuffer, 0, key0_luid6, key1_keyX));
	}

	@ReflectionExplicit
	@ReflectionEnumerable
	public long key0_luid6;

	@ReflectionExplicit
	@ReflectionEnumerable
	public Guid key1_keyX;

	@ReflectionExplicit
	@ReflectionEnumerable
	public byte val0_mode1;

	@ReflectionExplicit
	@ReflectionEnumerable
	public long val1_modified6;

	@ReflectionExplicit
	@ReflectionEnumerable
	public Guid val2_targetX;

	@Override
	public BaseObject basePrototype() {

		return RowTree.PROTOTYPE;
	}

	@ReflectionExplicit
	public String getVal0_mode1() {

		return TreeLinkType.valueForIndex(this.val0_mode1).name();
	}

	@ReflectionExplicit
	public BaseDate getVal1_modified6() {

		return Base.forDateMillis(this.val1_modified6 * 1000L);
	}

	@ReflectionExplicit
	public void setVal1_modified6(final Date date) {

		this.val1_modified6 = date.getTime() / 1000L;
	}
}
