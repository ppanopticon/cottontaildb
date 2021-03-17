package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.storage.engine.hare.basics.Page

/**
 * A [HareSerializer] for HARE based [Complex32Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Complex32ValueHareSerializer: HareSerializer<Complex32Value> {
    override val fixed: Boolean = true
    override fun serialize(page: Page, offset: Int, value: Complex32Value) {
        page.putFloat(offset, value.real.value)
        page.putFloat(offset, value.imaginary.value)
    }

    override fun deserialize(page: Page, offset: Int): Complex32Value = Complex32Value(page.getFloat(offset), page.getFloat(offset + Float.SIZE_BYTES))
    override fun deserialize(page: Page, offset: Int, size: Int): Array<Complex32Value> {
        val buffer = page.buffer.duplicate().position(offset)
        return Array(size) { Complex32Value(buffer.float, buffer.float) }
    }
}