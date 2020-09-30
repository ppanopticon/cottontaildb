package org.vitrivr.cottontail.math.knn.metrics

/**
 * Custom exception to throw when a distance cannot support this array configuration (too many / few).
 *
 * @author loris.sauter
 */
class ArrayConfigurationNotSupportedException : RuntimeException {

    constructor() : super()
    constructor(message: String?) : super(message)
    constructor(message: String?, cause: Throwable?) : super(message, cause)
    constructor(cause: Throwable?) : super(cause)
    constructor(message: String?, cause: Throwable?, enableSuppression: Boolean, writableStackTrace: Boolean) : super(
            message,
            cause,
            enableSuppression,
            writableStackTrace
    )
}
