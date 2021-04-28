package org.vitrivr.cottontail.database.queries.planning

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.database.queries.Digest
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.util.concurrent.locks.StampedLock


/**
 * This is simplistic data structure to maintain cached versions of [OperatorNode.Physical],
 * which are the equivalent of physical query plans..
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class CottontailPlanCache(val size: Int = 100) {

    /** The [Object2ObjectLinkedOpenHashMap] used to cache query plan implementations. */
    private val planCache = Object2ObjectLinkedOpenHashMap<Long, OperatorNode.Physical>(size)

    /** A lock used to mediate access to this [CottontailPlanCache]. */
    private val lock = StampedLock()

    /**
     * Retrieves an [OperatorNode.Physical] for the given [OperatorNode.Logical] from this [CottontailPlanCache].
     *
     * @param logical The [OperatorNode.Logical] to retrieve the [OperatorNode.Physical] for.
     */
    operator fun get(digest: Digest): OperatorNode.Physical? = this.lock.read { this.planCache[digest] }

    /**
     * Retrieves an [OperatorNode.Physical] for the given [OperatorNode.Logical] from this [CottontailPlanCache].
     *
     * @param logical The [OperatorNode.Logical] to retrieve the [OperatorNode.Physical] for.
     */
    operator fun get(logical: OperatorNode.Logical): OperatorNode.Physical? = get(logical.digest())

    /**
     * Registers a [OperatorNode.Physical] for the given [OperatorNode.Logical] to this [CottontailPlanCache].
     *
     * @param digest The [Digest] to register the [OperatorNode.Physical] for
     * @param physical The [OperatorNode.Physical] to register.
     */
    operator fun set(digest: Digest, physical: OperatorNode.Physical) = this.lock.write {
        if (this.planCache.size >= this.size) {
            this.planCache.remove(this.planCache.keys.first())
        }
        this.planCache[digest] = physical
    }

    /**
     * Registers a [OperatorNode.Physical] for the given [OperatorNode.Logical] to this [CottontailPlanCache].
     *
     * @param logical The [OperatorNode.Logical] to register the [OperatorNode.Physical] for
     * @param physical The [OperatorNode.Physical] to register.
     */
    operator fun set(logical: OperatorNode.Logical, physical: OperatorNode.Physical) = set(logical.digest(), physical)

    /**
     * Clears this [CottontailPlanCache] instance.
     */
    fun clear() = this.lock.write  {
        this.planCache.clear()
    }
}