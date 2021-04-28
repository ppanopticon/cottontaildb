package org.vitrivr.cottontail.legacy

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectMaps
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.Tx
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.TransactionManager.Transaction
import org.vitrivr.cottontail.execution.TransactionStatus
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.TransactionId
import org.vitrivr.cottontail.utilities.io.TxFileUtilities
import java.io.BufferedWriter
import java.nio.file.*
import java.util.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * An abstract implementation of the [MigrationManager].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@ExperimentalTime
abstract class AbstractMigrationManager(val batchSize: Int, logFile: Path) : MigrationManager {

    /** The internal [TransactionId] counter. */
    private val transactionIdCounter = AtomicLong(0L)

    /** [BufferedWriter] used to write to the log file. */
    private val writer: BufferedWriter = Files.newBufferedWriter(
        logFile.resolve("cottontaildb_migration_${System.currentTimeMillis()}.log"),
        StandardOpenOption.CREATE,
        StandardOpenOption.CREATE
    )

    /**
     * Tries to open the source [Catalogue] for migration.
     *
     * @param config The [Config] to open the [Catalogue] with.
     * @return Source [Catalogue] of null upon failure.
     */
    abstract fun openSourceCatalogue(config: Config): Catalogue?

    /**
     * Tries to open the destination [Catalogue] for migration.
     *
     * @param config The [Config] to open the [Catalogue] with.
     * @return Destination [Catalogue] of null upon failure.
     */
    abstract fun openDestinationCatalogue(config: Config): Catalogue?

    /**
     * Executes the data migration.
     */
    override fun migrate(config: Config) {
        /** Opens old catalogue. */
        val duration = measureTime {
            this.log("Starting catalogue migration from V${this.from} for ${config.root}.\n")
            val srcCatalogue: Catalogue? = this.openSourceCatalogue(config)
            if (srcCatalogue == null) {
                this.log("Failed to open source catalogue.\n")
                return
            } else {
                this.log("Source catalogue ${srcCatalogue.config.root} loaded successfully.\n")
            }

            /* Open and create new catalogue. */
            val migratedDatabaseRoot = config.root.parent.resolve("${config.root.fileName}~migrated")
            if (!Files.exists(migratedDatabaseRoot)) {
                Files.createDirectories(migratedDatabaseRoot)
            }
            val dstCatalogue: Catalogue? = this.openDestinationCatalogue(config.copy(root = migratedDatabaseRoot))
            if (dstCatalogue == null) {
                this.log("Failed to open destination catalogue.\n")
                srcCatalogue.close()
                return
            } else {
                this.log("Destination catalogue ${dstCatalogue.config.root} loaded successfully.\n")
            }

            /* Execute actual data migration. */
            try {
                /* Migrates all DBOs. */
                this.migrateDBOs(srcCatalogue, dstCatalogue)

                /* Migrates all data. */
                this.migrateData(srcCatalogue, dstCatalogue)

                /* Swap folders. */
                Files.move(config.root, config.root.parent.resolve("${config.root.fileName}~old"), StandardCopyOption.ATOMIC_MOVE)
                Files.move(migratedDatabaseRoot, config.root, StandardCopyOption.ATOMIC_MOVE)
            } catch (e: Throwable) {
                this.log("Error during data migration: ${e.message}\n")

                /* Delete destination (Cleanup). */
                TxFileUtilities.delete(dstCatalogue.path)
            } finally {
                /* Close catalogues. */
                srcCatalogue.close()
                dstCatalogue.close()
            }
        }
        this.log("Data migration completed. Took $duration.\n")
    }

    /**
     * Migrates all [DBO] from the source [Catalogue] to the destination [Catalogue].
     *
     * This is done in a single transaction!
     *
     * @param source The source [Catalogue].
     * @param destination The destination [Catalogue].
     */
    protected open fun migrateDBOs(source: Catalogue, destination: Catalogue) {
        /* Execute actual data migration. */
        val context = MigrationContext()
        val srcCatalogueTx = context.getTx(source) as CatalogueTx
        val dstCatalogueTx = context.getTx(destination) as CatalogueTx

        /* Migrate schemas. */
        val schemas = srcCatalogueTx.listSchemas()
        for ((s, srcSchema) in schemas.withIndex()) {
            this.log("+ Migrating schema ${srcSchema.name} (${s + 1} / ${schemas.size}):\n")

            val schema = dstCatalogueTx.createSchema(srcSchema.name)
            val dstSchemaTx = context.getTx(schema) as SchemaTx
            val srcSchemaTx = context.getTx(srcSchema) as SchemaTx
            val entities = srcSchemaTx.listEntities()

            /* Migrate entities. */
            for ((i, srcEntity) in entities.withIndex()) {
                this.log("-- Migrating entity ${srcEntity.name} (${i + 1} / ${entities.size}):\n")
                val srcEntityTx = context.getTx(srcEntity) as EntityTx
                val entity = dstSchemaTx.createEntity(srcEntity.name, *srcEntityTx.listColumns().map { it.columnDef to ColumnEngine.MAPDB }.toTypedArray())

                /* Migrate indexes. */
                for (index in srcEntityTx.listIndexes()) {
                    this.log("---- Migrating index ${index.name}...\n")
                    val destEntityTx = context.getTx(entity) as EntityTx
                    destEntityTx.createIndex(index.name, index.type, index.columns, index.config.toMap())
                }
            }
        }

        /* Commit after all DBOs have been rebuilt. */
        context.commit()
    }

    /**
     * Migrates a single [Entity], including all its [Column]s.
     *
     * This is just a default implementation that covers the standard case and can be overwritten.
     *
     * @param source The [CatalogueTx] pointing to the source catalogue.
     * @param destination The [CatalogueTx] pointing to the destination catalogue.
     */
    protected open fun migrateData(source: Catalogue, destination: Catalogue) {
        val sourceContext = MigrationContext()
        val srcCatalogueTx = sourceContext.getTx(source) as CatalogueTx
        val schemas = srcCatalogueTx.listSchemas()

        for ((s, srcSchema) in schemas.withIndex()) {
            val srcSchemaTx = sourceContext.getTx(srcSchema) as SchemaTx
            val entities = srcSchemaTx.listEntities()
            for ((e, srcEntity) in entities.withIndex()) {
                this.logStdout("+ Migrating data for schema ${srcSchema.name} (${s + 1} / ${schemas.size})...\n")

                val srcEntityTx = sourceContext.getTx(srcEntity) as EntityTx
                val count = srcEntityTx.count()
                val maxTupleId = srcEntityTx.maxTupleId()
                val columns = srcEntityTx.listColumns().map { it.columnDef }.toTypedArray()

                /* Start migrating column data. */
                if (count > 0) {
                    var i = 0L
                    val p = Math.floorDiv(maxTupleId, this.batchSize).toInt()
                    for (j in 0 until p) {
                        val context = MigrationContext()
                        val destCatalogueTx = context.getTx(destination) as CatalogueTx
                        val destSchemaTx = context.getTx(destCatalogueTx.schemaForName(srcSchema.name)) as SchemaTx
                        val destEntityTx = context.getTx(destSchemaTx.entityForName(srcEntity.name)) as EntityTx
                        srcEntityTx.scan(columns, j, p).forEach { r ->
                            this.logStdout("-- Migrating data for ${srcEntity.name}... (${++i} / $count)\r")
                            destEntityTx.insert(r)
                        }
                        this.log("-- Migrating data for ${srcEntity.name}; committing... (${i} / $count)\r")
                        context.commit()
                    }
                    this.log("-- Data migration for ${srcEntity.name} completed (${i} / $count).\n")
                } else {
                    this.log("-- Data migration for ${srcEntity.name} skipped (no data).\n")
                }
            }
        }

        /* Finalize source transaction. */
        sourceContext.commit()
    }

    /**
     * Logs a message to stdout and the log file.
     *
     * @param message The message to log.
     */
    protected fun logStdout(message: String) {
        print(message)
    }

    /**
     * Logs a message to stdout and the log file.
     *
     * @param message The message to log.
     */
    protected fun log(message: String) {
        print(message)
        this.writer.append(message)
        this.writer.flush()
    }

    /**
     * Closes this [MigrationManager].
     */
    override fun close() {
        this.writer.close()
    }

    /**
     * A [MigrationContext] is a special type of [TransactionContext] used during data migration.
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    inner class MigrationContext : TransactionContext {
        /** The [TransactionId] of the [MigrationContext]. */
        override val txId: TransactionId = transactionIdCounter.getAndIncrement()

        /** The [TransactionType] of a [MigrationManager] is always [TransactionType.SYSTEM]. */
        override val type: TransactionType = TransactionType.SYSTEM

        /** The [TransactionStatus] of this [MigrationContext]. */
        @Volatile
        override var state: TransactionStatus = TransactionStatus.READY
            private set

        /** Map of all [Tx] that have been created as part of this [MigrationManager]. Used for final COMMIT or ROLLBACK. */
        protected val txns: MutableMap<DBO, Tx> = Object2ObjectMaps.synchronize(Object2ObjectLinkedOpenHashMap())

        /**
         * Returns the [Tx] for the provided [DBO]. Creating [Tx] through this method makes sure,
         * that only on [Tx] per [DBO] and [Transaction] is created.
         *
         * @param dbo [DBO] to return the [Tx] for.
         * @return entity [Tx]
         */
        override fun getTx(dbo: DBO): Tx = this.txns.computeIfAbsent(dbo) {
            dbo.newTx(this)
        }

        override fun requestLock(dbo: DBO, mode: LockMode) {
            /* No op. */
        }

        /**
         * Since migrations cannot be executed on live-instances of Cottontail DB, the locks held on
         * any DB object is always [LockMode.EXCLUSIVE].
         */
        override fun lockOn(dbo: DBO): LockMode = LockMode.EXCLUSIVE

        /**
         *
         */
        override fun signalEvent(event: DataChangeEvent) {/* NoOp */
        }

        /**
         * Commits this [Transaction] thus finalizing and persisting all operations executed so far.
         */
        fun commit() {
            check(this.state === TransactionStatus.READY) { "Cannot commit transaction ${this.txId} because it is in wrong state (s = ${this.state})." }
            this.state = TransactionStatus.FINALIZING
            try {
                this.txns.values.reversed().forEachIndexed { i, txn ->
                    txn.commit()
                }
            } finally {
                this.txns.clear()
                this.state = TransactionStatus.COMMIT
            }
        }

        /**
         * Rolls back this [Transaction] thus reverting all operations executed so far.
         */
        fun rollback() {
            check(this.state === TransactionStatus.READY || this.state === TransactionStatus.ERROR) { "Cannot rollback transaction ${this.txId} because it is in wrong state (s = ${this.state})." }
            this.state = TransactionStatus.FINALIZING
            try {
                this.txns.values.reversed().forEach { txn ->
                    txn.rollback()
                }
            } finally {
                this.txns.clear()
                this.state = TransactionStatus.COMMIT
            }
        }
    }
}