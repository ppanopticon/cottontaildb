package org.vitrivr.cottontail.database.catalogue

import org.vitrivr.cottontail.database.general.TxSnapshot
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.model.basics.Name

/**
 * These is a [TxSnapshot] for [CatalogueTx]. It tracks all [Schema]s that have been created or
 * dropped during the [CatalogueTx]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface CatalogueTxSnapshot : TxSnapshot {
    /** A map of all [Schema] structures available to the enclosing [CatalogueTx]. */
    val schemas: MutableMap<Name.SchemaName, Schema>
}