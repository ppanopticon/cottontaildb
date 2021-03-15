package org.vitrivr.cottontail.database.general

/**
 * A [TxSnapshot] tracks changes made through a [Tx] that are local to the [Tx] and not visible to the 'outside'.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface TxSnapshot {

    /**
     * Processes a COMMIT decision and integrates changes made through the [Tx] into the enclosing [DBO].
     */
    fun commit()

    /**
     * Processes a ROLLBACK decision and undos changes made through the [Tx] into enclosing [DBO].
     */
    fun rollback()
}