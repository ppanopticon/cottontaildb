package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.mapdb.serializer.GroupSerializer
import org.vitrivr.cottontail.model.values.ShortValue
import org.vitrivr.cottontail.model.values.StringValue
import java.io.IOException
import java.util.*

/**
 * A [MapDBSerializer] for MapDB based [ShortValue] serialization and deserialization.
 *
 * Acts as a [GroupSerializer].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object StringValueMapDBSerializer : MapDBSerializer<StringValue>, GroupSerializer<StringValue> {
    @Throws(IOException::class)
    override fun serialize(out: DataOutput2, value: StringValue) {
        out.writeUTF(value.value)
    }

    @Throws(IOException::class)
    override fun deserialize(`in`: DataInput2, available: Int): StringValue {
        return StringValue(`in`.readUTF())
    }

    override fun isTrusted(): Boolean {
        return true
    }

    @Throws(IOException::class)
    override fun valueArraySerialize(out2: DataOutput2, vals: Any) {
        for (v in vals as Array<CharArray>) {
            out2.packInt(v.size)
            for (c in v) {
                out2.packInt(c.toInt())
            }
        }
    }

    @Throws(IOException::class)
    override fun valueArrayDeserialize(in2: DataInput2, size: Int): Array<CharArray> {
        return Array(size) {
            val size2 = in2.unpackInt()
            val cc = CharArray(size2)
            for (j in 0 until size2) {
                cc[j] = in2.unpackInt().toChar()
            }
            cc
        }
    }

    override fun valueArraySearch(keys: Any, key: StringValue): Int {
        val key2 = key.value.toCharArray()
        return Arrays.binarySearch(keys as Array<CharArray>, key2, Serializer.CHAR_ARRAY)
    }

    override fun valueArraySearch(vals: Any, key: StringValue, comparator: Comparator<Any>): Int {
        val array = vals as Array<CharArray>
        var lo = 0
        var hi = array.size - 1
        while (lo <= hi) {
            val mid = lo + hi ushr 1
            val compare = comparator.compare(key.value, String(array[mid]))
            if (compare == 0) return mid else if (compare < 0) hi = mid - 1 else lo = mid + 1
        }
        return -(lo + 1)
    }

    override fun valueArrayGet(vals: Any, pos: Int): StringValue {
        return StringValue(String((vals as Array<CharArray?>)[pos]!!))
    }

    override fun valueArraySize(vals: Any): Int {
        return (vals as Array<CharArray?>).size
    }

    override fun valueArrayEmpty(): Array<CharArray?> {
        return arrayOfNulls(0)
    }

    override fun valueArrayPut(vals: Any, pos: Int, newValue: StringValue): Array<CharArray> {
        val array = vals as Array<CharArray>
        val ret = Arrays.copyOf(array, array.size + 1)
        if (pos < array.size) {
            System.arraycopy(array, pos, ret, pos + 1, array.size - pos)
        }
        ret[pos] = newValue.value.toCharArray()
        return ret
    }

    override fun valueArrayUpdateVal(vals: Any, pos: Int, newValue: StringValue): Array<CharArray?> {
        var vals2 = vals as Array<CharArray?>
        vals2 = vals2.clone()
        vals2[pos] = newValue.value.toCharArray()
        return vals2
    }

    override fun valueArrayFromArray(objects: Array<Any>): Array<CharArray?> = Array(objects.size) {
        (objects[it] as StringValue).value.toCharArray()
    }

    override fun valueArrayCopyOfRange(vals: Any, from: Int, to: Int): Array<CharArray> = (vals as Array<CharArray>).copyOfRange(from, to)

    override fun valueArrayDeleteValue(vals: Any, pos: Int): Array<CharArray?> {
        val vals2 = arrayOfNulls<CharArray>((vals as Array<CharArray?>).size - 1)
        System.arraycopy(vals, 0, vals2, 0, pos - 1)
        System.arraycopy(vals, pos, vals2, pos - 1, vals2.size - (pos - 1))
        return vals2
    }

    override fun hashCode(s: StringValue, seed: Int): Int {
        val c = s.value.toCharArray()
        return Serializer.CHAR_ARRAY.hashCode(c, seed)
    }

    override fun nextValue(value: StringValue): StringValue {
        val array = value.value.toCharArray()
        return StringValue(String(Serializer.CHAR_ARRAY.nextValue(array)))
    }
}