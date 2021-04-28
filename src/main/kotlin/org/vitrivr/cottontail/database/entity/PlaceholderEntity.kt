package org.vitrivr.cottontail.database.entity

import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.DBOVersion
import org.vitrivr.cottontail.database.general.Placeholder
import org.vitrivr.cottontail.database.general.PlaceholderType
import org.vitrivr.cottontail.database.schema.DefaultSchema
import org.vitrivr.cottontail.database.statistics.entity.EntityStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.TupleId
import java.nio.file.Path

/**
 * Represents a placeholder for a single entity in the Cottontail DB data model. An [PlaceholderEntity] cannot be
 * used to initiate [EntityTx] nor can it be used to query any property of the underlying [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class PlaceholderEntity(override val path: Path, override val parent: DefaultSchema, override val type: PlaceholderType) : Entity, Placeholder {
    override val name: Name.EntityName = parent.name.entity(this.path.fileName.toString().replace("entity_", ""))
    override val statistics: EntityStatistics = EntityStatistics()
    override val numberOfColumns: Int = 0
    override val numberOfRows: Long = 0L
    override val maxTupleId: TupleId = -1L
    override val closed: Boolean = true

    /** The [DBOVersion] of a [PlaceholderEntity] is inherited by its parent. */
    override val version: DBOVersion
        get() = this.parent.version

    override fun newTx(context: TransactionContext): EntityTx {
        throw UnsupportedOperationException("Broken entity ${this.name} cannot be used to start a new transaction.")
    }

    /**
     * Tries to initialize and actual [DefaultEntity] instance for this [PlaceholderEntity].
     * Returns [DefaultEntity] on success and throws an [Exception] otherwise.
     *
     * @return [DefaultEntity]
     * @throws [Exception] If [DefaultEntity] initialization fails.
     */
    override fun initialize(): DBO = DefaultEntity(this.path, this.parent)


    override fun close() { /* NoOp. */
    }
}