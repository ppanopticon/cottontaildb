package org.vitrivr.cottontail.database.schema

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.TxSnapshot
import org.vitrivr.cottontail.model.basics.Name

/**
 * These is a [TxSnapshot] for [SchemaTx]. It tracks all [Entity] that have been created or dropped during the [SchemaTx]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface SchemaTxSnapshot : TxSnapshot {
    /** A map of all [Entity] structures available to the enclosing [SchemaTx]. */
    val entities: MutableMap<Name.EntityName, Entity>
}