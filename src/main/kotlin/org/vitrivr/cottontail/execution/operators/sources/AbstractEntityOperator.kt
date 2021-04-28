package org.vitrivr.cottontail.execution.operators.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.execution.operators.basics.Operator

/**
 * An abstract [Operator.SourceOperator] that accesses an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
abstract class AbstractEntityOperator(groupId: GroupId, protected val entity: EntityTx, override val columns: Array<ColumnDef<*>>) : Operator.SourceOperator(groupId)