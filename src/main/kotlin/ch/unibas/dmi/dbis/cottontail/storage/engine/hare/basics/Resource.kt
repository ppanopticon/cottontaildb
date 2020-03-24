package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.basics

/**
 * An abstract [Resource] that can be closed, i.e., that extends [AutoCloseable]. However, [Resource]s
 * foresee a mechanism to query the [Resource]'s state.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface Resource : AutoCloseable {
    /** A flag indicating whether this [Resource] is still open and thus save for use. */
    val isOpen: Boolean
}