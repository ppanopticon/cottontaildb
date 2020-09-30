package org.vitrivr.cottontail.execution.operators.basics

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator] used during query execution.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
abstract class Operator(val context: ExecutionEngine.ExecutionContext) : AutoCloseable {
    /** Status of the [Operator]. */
    var status: OperatorStatus = OperatorStatus.CREATED
        protected set

    /** The list of [ColumnDef]s produced by this [Operator]. */
    abstract val columns: Array<ColumnDef<*>>

    /**
     * Opens this [Operator], i.e., signals the impending start of query execution. If there
     * is a parent [Operator], then this call is usually propagated.
     */
    abstract fun open()

    /**
     * Converts this [Operator] to a [Flow] and returns it.
     *
     * @param scope The [CoroutineScope] used for execution
     * @return [Flow]
     *
     * @throws IllegalStateException If this [Operator.status] is not [OperatorStatus.OPEN]
     */
    abstract fun toFlow(scope: CoroutineScope): Flow<Record>
}