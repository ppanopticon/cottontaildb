package org.vitrivr.cottontail.database.entity

import org.vitrivr.cottontail.model.basics.TupleId

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
data class EntityStatistics(val columns: Int, val rows: Long, val maxTupleId: TupleId)
