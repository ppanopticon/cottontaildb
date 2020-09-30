package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.OperatorStatus
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.LongValue

/**
 * An [AbstractEntityOperator] that counts the number of entries in an [Entity] and returns one [Record] with that number.
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
class EntityCountOperator(context: ExecutionEngine.ExecutionContext, entity: Entity) : AbstractEntityOperator(context, entity, arrayOf(entity.allColumns().first())) {

    /** The [ColumnDef] returned by this [EntitySampleOperator]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(ColumnDef.withAttributes(entity.name.column("count()"), "LONG"))

    /**
     * Converts this [EntityCountOperator] to a [Flow] and returns it.
     *
     * @param scope The [CoroutineScope] used for execution
     * @return [Flow] representing this [EntityCountOperator]
     * @throws IllegalStateException If this [Operator.status] is not [OperatorStatus.OPEN]
     */
    override fun toFlow(scope: CoroutineScope): Flow<Record> = flow {
        emit(StandaloneRecord(0L, this@EntityCountOperator.columns, arrayOf(LongValue(context.getTx(entity).count()))))
    }
}