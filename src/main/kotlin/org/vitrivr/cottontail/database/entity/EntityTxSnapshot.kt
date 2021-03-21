package org.vitrivr.cottontail.database.entity

import org.vitrivr.cottontail.database.general.TxSnapshot
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.statistics.entity.EntityStatistics
import org.vitrivr.cottontail.model.basics.Name

/**
 * This is a [TxSnapshot] implementation for [EntityTx]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface EntityTxSnapshot : TxSnapshot {
    /** The state of the enclosing [Entity] as of the start of the [EntityTx]. */
    val statistics: EntityStatistics

    /** A map of all [Index] structures available to the enclosing [EntityTx]. */
    val indexes: MutableMap<Name.IndexName, Index>

    /** A map of all [Index] structures created by the enclosing [EntityTx]. */
    val created: MutableMap<Name.IndexName, Index>

    /** A map of all [Index] structures dropped by the enclosing [EntityTx]. */
    val dropped: MutableMap<Name.IndexName, Index>
}