package org.vitrivr.cottontail.storage.engine.hare.views

import org.vitrivr.cottontail.storage.engine.hare.basics.Page

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface PageView {
    /** [Page] wrapped by this [PageView]. */
    val page: Page

    /** Returns the [Page] identifier for the [Page] wrapped by this [PageView]. */
    val pageTypeIdentifier
        get() = this.page.getInt(0)
}