package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.utilities.extensions.read

/**
 * A [HareSerializer] for HARE based [Complex32Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Complex32ValueHareSerializer: HareSerializer<Complex32Value> {
    override val fixed: Boolean = true
    override fun serialize(page: HarePage, offset: Int, value: Complex32Value) {
        page.putFloat(offset, value.real.value)
        page.putFloat(offset, value.imaginary.value)
    }
    override fun deserialize(page: HarePage, offset: Int): Complex32Value = Complex32Value(page.getFloat(offset), page.getFloat(offset + Float.SIZE_BYTES))
    override fun deserialize(page: HarePage, offset: Int, size: Int): Array<Complex32Value> = page.lock.read {
        val buffer = page.buffer.duplicate().position(offset)
        Array(size) { Complex32Value(buffer.float, buffer.float) }
    }
}