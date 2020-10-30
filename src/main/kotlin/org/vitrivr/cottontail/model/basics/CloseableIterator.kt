package org.vitrivr.cottontail.model.basics

import org.vitrivr.cottontail.storage.engine.hare.basics.Resource

/**
 * An [Iterator] that must be closed, because it's using and potentially blocking some underlying resource.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface CloseableIterator<T> : Resource, Iterator<T>