package org.vitrivr.cottontail.storage.engine.hare.access.interfaces

import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.storage.engine.hare.basics.Resource
import org.vitrivr.cottontail.storage.engine.hare.disk.HareDiskManager

/**
 * A HARE column file that can hold arbitrary [Value] data.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface HareColumnFile<T : Value> : Resource {
    /** The [HareDiskManager] that backs this [HareColumnFile]. */
    val disk: HareDiskManager

    /** The [Name] of this [HareColumnFile]. */
    val name: Name.ColumnName

    /** The [ColumnDef] describing the column managed by this [HareColumnFile]. */
    val columnDef: ColumnDef<T>
}