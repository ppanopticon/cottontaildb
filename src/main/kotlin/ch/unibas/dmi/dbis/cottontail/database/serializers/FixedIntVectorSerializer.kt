package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.IntVectorValue
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics.Page
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.serializer.Serializer
import org.mapdb.DataInput2
import org.mapdb.DataOutput2

/**
 * A [Serializer] for [IntVectorValue]s that are fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedIntVectorSerializer(override val logicalSize: Int): Serializer<IntVectorValue> {
    override val physicalSize: Int = (this.logicalSize shl 2)

    override fun serialize(out: DataOutput2, value: IntVectorValue) {
        for (i in 0 until this.logicalSize) {
            out.writeInt(value[i].value)
        }
    }
    override fun deserialize(input: DataInput2, available: Int): IntVectorValue {
        val vector = IntArray(this.logicalSize)
        for (i in 0 until this.logicalSize) {
            vector[i] = input.readInt()
        }
        return IntVectorValue(vector)
    }

    override fun serialize(page: Page, offset: Int, value: IntVectorValue) {
        page.putInts(offset, value.data)
    }

    override fun deserialize(page: Page, offset: Int): IntVectorValue = IntVectorValue(page.getInts(offset, IntArray(this.logicalSize)))
}