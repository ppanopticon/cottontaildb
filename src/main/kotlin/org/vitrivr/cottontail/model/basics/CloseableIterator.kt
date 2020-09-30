package org.vitrivr.cottontail.model.basics

/**
 * An [Iterator] that must be closed, because it's using and potentially blocking some underlying resource.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface CloseableIterator<T> : AutoCloseable, Iterator<T>