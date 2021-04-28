package org.vitrivr.cottontail.database.general

/**
 * A [TxAction] represents an action executed during a [Tx].
 *
 * [TxAction] instances can be seen as in-memory WAL entries, that have not
 * yet been materialized to the `outside``
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface TxAction {

    /**
     * Processes a COMMIT decision and integrates changes made in this [TxAction] into the enclosing [DBO].
     */
    fun commit()

    /**
     * Processes a ROLLBACK decision and reverts changes made by this [TxAction].
     */
    fun rollback()
}