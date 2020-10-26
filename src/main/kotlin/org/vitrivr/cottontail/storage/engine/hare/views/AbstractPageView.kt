package org.vitrivr.cottontail.storage.engine.hare.views

import org.vitrivr.cottontail.storage.engine.hare.basics.Page
import org.vitrivr.cottontail.storage.engine.hare.basics.PageConstants

/**
 * [AbstractPageView]s provide a certain view onto a [Page] object, i.e., allow a specific mode of interaction,
 * depending on the concrete implementation. [AbstractPageView]s can be used to wrap or initialize [Page]s.

 * [AbstractPageView] implementation's state should only depend on the [Page] that is currently wrapped,
 * that is, [AbstractPageView] instances can be re-used with different [Page]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractPageView {

    /** The identifier for this [AbstractPageView]*/
    abstract val pageTypeIdentifier: Int

    /** The [Page] that is currently wrapped by this [AbstractPageView]. May be null*/
    var page: Page? = null
        protected set

    /**
     * Tries to wrap the given [Page] in this [AbstractPageView]. Does not change the [Page] when
     * doing so.
     *
     * This method is supposed to perform sanity checks before wrapping a [Page]. If these
     * sanity checks fail, a [IllegalArgumentException] should be thrown.
     *
     * @param page The [Page] that should be wrapped.
     * @return This [AbstractPageView]
     * @throws IllegalArgumentException If the provided [Page] is incompatible with this [AbstractPageView]
     */
    open fun wrap(page: Page): AbstractPageView {
        val type = page.getInt(0)
        require(type == this.pageTypeIdentifier) { "Cannot wrap page of type $type as ${this.javaClass.simpleName} (type = ${this.pageTypeIdentifier})." }
        this.page = page
        return this
    }

    /**
     * Tries to initialize and wrap the given [Page] with this [AbstractPageView]. Initializing a [Page]
     * means preparing it to be used with the current [AbstractPageView].
     *
     * Only uninitialized [Page]s should be initialized! This method is supposed to perform sanity
     * checks before initializing a [Page]. If these sanity checks fail, a [IllegalArgumentException]
     * should be thrown.
     *
     * @param page The [Page] that should be wrapped.
     * @return This [AbstractPageView]
     * @throws IllegalArgumentException If the provided [Page] is incompatible with this [AbstractPageView]
     */
    open fun initializeAndWrap(page: Page): AbstractPageView {
        val type = page.getInt(0)
        require(type == PageConstants.PAGE_TYPE_UNINITIALIZED) { "Cannot initialize page of type $type as ${this.javaClass.simpleName} (type = ${this.pageTypeIdentifier})." }
        page.putInt(0, this.pageTypeIdentifier)
        this.page = page
        return this
    }
}