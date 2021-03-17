package org.vitrivr.cottontail.storage.serializers.hare

import org.vitrivr.cottontail.model.values.Complex64Value
import org.vitrivr.cottontail.storage.engine.hare.basics.Page

/**
 * A [HareSerializer] for HARE based [Complex64Value] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Complex64ValueHareSerializer: HareSerializer<Complex64Value> {
    override val fixed: Boolean = true
    override fun serialize(page: Page, offset: Int, value: Complex64Value) {
        page.putDouble(offset, value.real.value)
        page.putDouble(offset, value.imaginary.value)
    }

    override fun deserialize(page: Page, offset: Int): Complex64Value = Complex64Value(page.getFloat(offset), page.getDouble(offset + Double.SIZE_BYTES))
    override fun deserialize(page: Page, offset: Int, size: Int): Array<Complex64Value> {
        val buffer = page.buffer.duplicate().position(offset)
        return Array(size) { Complex64Value(buffer.double, buffer.double) }
    }
}