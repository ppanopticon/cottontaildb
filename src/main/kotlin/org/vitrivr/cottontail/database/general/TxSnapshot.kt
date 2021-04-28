package org.vitrivr.cottontail.database.general

/**
 * A [TxSnapshot] tracks changes made through a [Tx] that are local to the [Tx] and not visible to the 'outside' yet.
 *
 * Most importantly, [TxSnapshot]s record a list of [TxAction]s, i.e., act as some sort of in-memory WAL. In addition,
 * though, [TxSnapshot] can also the [Tx] local state of a [DBO].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface TxSnapshot {
    /** A map of all [TxAction] recorded by this [TxSnapshot]. */
    val actions: List<TxAction>

    /**
     * Processes a COMMIT decision and integrates changes made through the [Tx] into the enclosing [DBO].
     */
    fun commit()

    /**
     * Processes a ROLLBACK decision and undos changes made through the [Tx] into enclosing [DBO].
     */
    fun rollback()

    /**
     * Records a [TxAction] with this [TxSnapshot].
     *
     * @param action The [TxAction] to record.
     * @return True on success, false otherwise.
     */
    fun record(action: TxAction): Boolean
}