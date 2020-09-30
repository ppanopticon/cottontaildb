package org.vitrivr.cottontail.execution.operators.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.SourceOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * An abstract [SourceOperator] that access an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
abstract class AbstractEntityOperator(context: ExecutionEngine.ExecutionContext, protected val entity: Entity, override val columns: Array<ColumnDef<*>>) : SourceOperator(context) {
    override fun prepareOpen() {
        this.context.prepareTransaction(this.entity, true)
    }

    override fun prepareClose() {
        /* NoOp. */
    }
}