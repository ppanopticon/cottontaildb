package org.vitrivr.cottontail.database.entity

/**
 * This class represents the local state of a [EntityTx]. Some changes made through such a [EntityTx],
 * are only visible to the [EntityTx] itself until that [EntityTx] is committed. These changes are
 * tracked in the [EntityTxState] object.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class EntityTxState