package org.vitrivr.cottontail.storage.serializers.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.mapdb.serializer.GroupSerializer
import org.vitrivr.cottontail.model.values.BooleanValue
import java.util.*

/**
 * A [MapDBSerializer] for MapDB based [BooleanValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object BooleanValueMapDBSerializer : MapDBSerializer<BooleanValue>, GroupSerializer<BooleanValue> {
    override fun deserialize(input: DataInput2, available: Int): BooleanValue = BooleanValue(input.readBoolean())
    override fun serialize(out: DataOutput2, value: BooleanValue) {
        out.writeBoolean(value.value)
        Serializer.BOOLEAN
    }

    override fun fixedSize(): Int = 1
    override fun isTrusted(): Boolean = true


    override fun valueArraySearch(keys: Any, key: BooleanValue): Int {
        return Arrays.binarySearch(valueArrayToArray(keys), key)
    }

    override fun valueArraySearch(keys: Any, key: BooleanValue, comparator: Comparator<*>): Int {
        return Arrays.binarySearch(valueArrayToArray(keys), key, comparator as Comparator<Any>)
    }

    override fun valueArraySerialize(out: DataOutput2, vals: Any) {
        for (b in vals as BooleanArray) {
            out.writeBoolean(b)
        }
    }

    override fun valueArrayDeserialize(`in`: DataInput2, size: Int): Any {
        val ret = BooleanArray(size)
        for (i in 0 until size) {
            ret[i] = `in`.readBoolean()
        }
        return ret
    }

    override fun valueArrayGet(vals: Any, pos: Int): BooleanValue {
        return BooleanValue((vals as BooleanArray)[pos])
    }

    override fun valueArraySize(vals: Any): Int {
        return (vals as BooleanArray).size
    }

    override fun valueArrayEmpty(): Any {
        return BooleanArray(0)
    }

    override fun valueArrayPut(vals: Any, pos: Int, newValue: BooleanValue): Any {
        val array = vals as BooleanArray
        val ret = array.copyOf(array.size + 1)
        if (pos < array.size) {
            System.arraycopy(array, pos, ret, pos + 1, array.size - pos)
        }
        ret[pos] = newValue.value
        return ret
    }

    override fun valueArrayUpdateVal(vals: Any, pos: Int, newValue: BooleanValue): Any {
        val vals2 = (vals as BooleanArray).clone()
        vals2[pos] = newValue.value
        return vals2
    }

    override fun valueArrayFromArray(objects: Array<Any>): Any {
        val ret = BooleanArray(objects.size)
        for (i in ret.indices) {
            ret[i] = objects[i] as Boolean
        }
        return ret
    }

    override fun valueArrayCopyOfRange(vals: Any?, from: Int, to: Int): Any? {
        return Arrays.copyOfRange(vals as BooleanArray, from, to)
    }

    override fun valueArrayDeleteValue(vals: Any, pos: Int): Any {
        val valsOrig = vals as BooleanArray
        val vals2 = BooleanArray(valsOrig.size - 1)
        System.arraycopy(vals, 0, vals2, 0, pos - 1)
        System.arraycopy(vals, pos, vals2, pos - 1, vals2.size - (pos - 1))
        return vals2
    }
}