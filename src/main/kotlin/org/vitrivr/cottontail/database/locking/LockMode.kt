package org.vitrivr.cottontail.database.locking

/**
 * Types of [Lock]s available to Cottontail DB.
 *
 * Inspired by: https://github.com/dstibrany/LockManager
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
enum class LockMode {
    /** No lock on the database object. Merely informative, trying to acquiring this type of lock is considered a programmer's error! */
    NO_LOCK,

    /** A shared lock on the object. Can be acquired. */
    SHARED,

    /** An exclusive on the object. Can be acquired. */
    EXCLUSIVE
}