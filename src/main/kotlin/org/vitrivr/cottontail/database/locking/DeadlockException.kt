package org.vitrivr.cottontail.database.locking

/**
 * A [Throwable] thrown whenever a deadlock is excepted while acquiring a lock.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DeadlockException constructor(l1: LockHolder, l2: List<LockHolder>) : Throwable("Deadlock detected while acquiring lock for transaction ${l1.txId}: ${l2.map { it.txId }.joinToString(", ")} -> ${l1.txId}")