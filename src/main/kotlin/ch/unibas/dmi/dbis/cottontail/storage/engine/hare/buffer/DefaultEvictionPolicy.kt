package ch.unibas.dmi.dbis.cottontail.storage.engine.hare.buffer

/**
 * Default [EvictionPolicy]. Orders considers [ufferPool.PageRef] based on [Priority] and access frequency.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object DefaultEvictionPolicy : EvictionPolicy {
  override fun compare(o1: BufferPool.PageRef, o2: BufferPool.PageRef): Int = when {
        o1.isEligibleForGc && o2.isEligibleForGc -> {
            if (o1.priority == o2.priority) {
                o1.lastAccess.compareTo(o2.lastAccess)
            } else {
                o1.priority.compareTo(o2.priority)
            }
        }
        o1.isEligibleForGc -> -1
        o2.isEligibleForGc -> 1
        else -> 0
    }
}