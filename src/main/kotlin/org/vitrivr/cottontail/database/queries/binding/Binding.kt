package org.vitrivr.cottontail.database.queries.binding

import org.vitrivr.cottontail.database.queries.QueryContext

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface Binding<T : Any?> {
    /**
     * Returns value [T] for this [Binding] given the [QueryContext].
     *
     * @param context [QueryContext] to use to resolve this [Binding].
     * @return [T]
     */
    fun apply(context: QueryContext): T
}