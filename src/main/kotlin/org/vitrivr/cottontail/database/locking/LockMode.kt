package org.vitrivr.cottontail.database.locking

/**
 * Types of [Lock]s available to Cottontail DB.
 *
 * Inspired by: https://github.com/dstibrany/LockManager
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class LockMode {
    NO_LOCK,

    /** No lock on the database object. Merely informative, trying to acquiring this type of lock is considered a programmer's error! */
    SHARED,

    /** A shared lock on the database object. Can be acquired. */
    EXCLUSIVE
    /** An exclusive on the database object. Can be acquired. */
}