package org.vitrivr.cottontail.database.queries.binding

/**
 * A [Binding] is a placeholder for content, usually a value, which will be populated at a later stage.
 *
 * Used in query binding and planning.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Binding<T : Any>(val index: Int) {

    /** The [BindingContext] currently associated with this [Binding]. The [BindingContext] can change! */
    internal var context: BindingContext<T>? = null

    /** The value [T] associated with this [Binding]. */
    val value: T
        get() = this.context?.get(this) ?: throw IllegalStateException("Failed to resolve value binding $this (context = $context).")

    override fun toString(): String = ":$index"
}