package ch.unibas.dmi.dbis.cottontail.calcite.rules

import org.apache.calcite.rex.RexInputRef
import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.rex.RexVisitorImpl
import org.apache.calcite.rex.RexNode


/**
 * Translator from [RexNode] to name's as accepted by Cottontail DB.
 */
class RexToCottontailTranslator(private val typeFactory: JavaTypeFactory, private val inFields: List<String>) : RexVisitorImpl<String>(true) {
    override fun visitInputRef(inputRef: RexInputRef): String = this.inFields[inputRef.index]
}