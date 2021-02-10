package org.vitrivr.cottontail.database.queries

/**
 * A [Binding] is a placeholder for content, usually a value, which will be populated at a later stage.
 * Any [Node] can hold any type of value.
 *
 * Used in query binding and planning.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Binding<T : Any?> {
    /**
     * Returns value [T] for this [Binding] given the [QueryContext].
     *
     * @param context [QueryContext] to use to resolve this [Binding].
     * @return [T]
     */
    fun bind(context: QueryContext): T
}