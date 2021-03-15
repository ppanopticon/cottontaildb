package org.vitrivr.cottontail.database.queries.binding

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A context for late binding of values. Late binding is used during query planning or execution,
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class BindingContext<T : Any> {

    /** List of bound [Value]s for this [QueryContext]. */
    private var bound = Object2ObjectOpenHashMap<Binding<T>, T?>()

    /** The size of this [BindingContext], i.e., the number of values bound. */
    val size: Int
        get() = this.bound.size

    /**
     * Returns the [Value] for the given [Binding].
     *
     * @param binding The [Binding] to lookup.
     * @return The bound [Value].
     */
    operator fun get(binding: Binding<T>): T? {
        require(this.bound.contains(binding)) { "Binding $binding is not known to this binding context." }
        return this.bound[binding]
    }

    /**
     * Updates the [Value] to an existing [Binding] in this [BindingContext].
     *
     * @param value The [Value] to bind. Can be null
     * @return The [Binding].
     */
    fun update(binding: Binding<T>, value: T?) {
        require(binding.context == this) { "Binding $binding does not belong to this binding context." }
        require(this.bound.contains(binding)) { "Binding $binding is not known to this binding context." }
        this.bound[binding] = value
    }

    /**
     * Registers the [Value] with the given [Binding] in this [BindingContext].
     *
     * @param value The [Value] to bind. Can be null
     * @return The [Binding].
     */
    fun register(binding: Binding<T>, value: T?): Binding<T> {
        require(binding.context == null) { "Binding $binding has been registered with another binding context." }
        this.bound[binding] = value
        binding.context = this
        return binding
    }

    /**
     * Binds a [Value] to this [BindingContext].
     *
     * @param value The [Value] to bind. Can be null
     * @return The [Binding].
     */
    fun bind(value: T?): Binding<T> = this.register(Binding(this.bound.size), value)

    /**
     * Clears this [BindingContext] and severs all existing connections to [Binding]s.
     */
    fun clear() {
        for (bound in this.bound.keys) {
            bound.context = null
        }
        this.bound.clear()
    }
}