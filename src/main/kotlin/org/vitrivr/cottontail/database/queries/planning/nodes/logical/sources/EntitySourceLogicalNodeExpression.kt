package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalNodeExpression
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class EntitySourceLogicalNodeExpression(val entity: Entity, val columns: Array<ColumnDef<*>>) : NullaryLogicalNodeExpression()