package org.vitrivr.cottontail.execution.operators.basics

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlin.coroutines.cancellation.CancellationException


class AbortFlowException constructor(val owner: FlowCollector<*>) : CancellationException("Flow was aborted, no more elements needed") {
    fun checkOwnership(owner: FlowCollector<*>) {
        if (this.owner !== owner) throw this
    }
}

/**
 * Returns a flow that ignores first [count] elements.
 * Throws [IllegalArgumentException] if [count] is negative.
 */
fun <T> Flow<T>.drop(count: Long): Flow<T> {
    require(count >= 0L) { "Drop count should be non-negative, but had $count" }
    return flow {
        var skipped = 0L
        collect { value ->
            if (skipped >= count) emit(value) else ++skipped
        }
    }
}

/**
 * Returns a flow that contains first [count] elements.
 * When [count] elements are consumed, the original flow is cancelled.
 * Throws [IllegalArgumentException] if [count] is not positive.
 */
fun <T> Flow<T>.take(count: Long): Flow<T> {
    require(count > 0L) { "Requested element count $count should be positive" }
    return flow {
        var consumed = 0
        try {
            collect { value ->
                if (++consumed < count) {
                    return@collect emit(value)
                } else {
                    emit(value)
                    throw AbortFlowException(this)
                }
            }
        } catch (e: AbortFlowException) {
            e.checkOwnership(owner = this)
        }
    }
}