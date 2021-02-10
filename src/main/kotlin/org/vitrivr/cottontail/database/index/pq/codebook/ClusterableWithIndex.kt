package org.vitrivr.cottontail.database.index.pq.codebook

import org.apache.commons.math3.ml.clustering.Clusterable

/**
 * A [Clusterable] implementation that also contains an index. Used for [PQCodebook] implementations.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
interface ClusterableWithIndex : Clusterable {
    fun getIndex(): Int
}