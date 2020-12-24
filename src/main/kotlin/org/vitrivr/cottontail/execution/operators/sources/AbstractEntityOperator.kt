package org.vitrivr.cottontail.execution.operators.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * An abstract [Operator.SourceOperator] that accesses an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
abstract class AbstractEntityOperator(protected val entity: Entity, override val columns: Array<ColumnDef<*>>) : Operator.SourceOperator()