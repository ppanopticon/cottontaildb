package org.vitrivr.cottontail.database.index.vaplus

import org.apache.commons.math3.linear.MatrixUtils
import org.mapdb.Atomic
import org.mapdb.DBMaker
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTransaction
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.hash.UniqueHashIndex
import org.vitrivr.cottontail.database.index.lsh.superbit.SuperBitLSHIndex
import org.vitrivr.cottontail.database.queries.components.AtomicBooleanPredicate
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path

/**
 * Represents a VAF based index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to
 * index one to many [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return
 * [Recordset]s.
 *
 * TODO: Fix and finalize implementation.
 *
 * @author Manuel Huerbin
 * @version 1.1.2
 */
class VAPlusIndex(override val name: Name.IndexName, override val parent: Entity, override val columns: Array<ColumnDef<*>>) : Index() {

    /**
     * Index-wide constants.
     */
    companion object {
        const val META_FIELD_NAME = "vaf_meta"
        const val SIGNATURE_FIELD_NAME = "vaf_signatures"
        private val LOGGER = LoggerFactory.getLogger(VAPlusIndex::class.java)
    }

    /** Path to the [VAPlusIndex] file. */
    override val path: Path = this.parent.path.resolve("idx_vaf_$name.db")

    /** The type of [Index] */
    override val type: IndexType = IndexType.VAF

    /** True since [VAPlusIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** The [VAPlusIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.parent.name.column("distance"), ColumnType.forName("DOUBLE")))

    /** The internal [DB] reference. */
    private val db = if (parent.parent.parent.config.memoryConfig.forceUnmapMappedFiles) {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().cleanerHackEnable().transactionEnable().make()
    } else {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().transactionEnable().make()
    }

    /** Map structure used for [VAPlusIndex]. */
    private val meta: Atomic.Var<VAPlusMeta> = this.db.atomicVar(META_FIELD_NAME, VAPlusMetaSerializer).createOrOpen()
    private val signatures = this.db.indexTreeList(SIGNATURE_FIELD_NAME, VAPlusSignatureSerializer).createOrOpen()

    /**
     * Flag indicating if this [VAPlusIndex] has been closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [VAPlusIndex].
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = if (predicate is KnnPredicate<*>) {
        // TODO
        predicate.columns.first() == this.columns[0]
    } else {
        false
    }

    /**
     * Calculates the cost estimate of this [VAPlusIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = when {
        predicate !is KnnPredicate<*> || predicate.columns.first() != this.columns[0] -> Cost.INVALID
        else -> Cost.ZERO /* TODO: Determine. */
    }

    /**
     * Calculates bound for given signature.
     *
     * @param cells Signature for the vector from the collection.
     * @param bounds Vector containing bounds for query vector.
     * @param boundsIndex Vector containing indexes for bounds of query vector.
     */
    private fun calculateBounds(cells: IntArray, bounds: FloatArray, boundsIndex: IntArray) = cells.mapIndexed { i, it ->
        val cellsIdx = if (it < 0) {
            ((Short.MAX_VALUE + 1) * 2 + it).toShort().toInt()
        } else {
            it
        }
        bounds[boundsIndex[i] + cellsIdx]
    }.sum()

    /**
     * Opens and returns a new [IndexTransaction] object that can be used to interact with this [Index].
     *
     * @param parent If the [Entity.Tx] that requested the [IndexTransaction].
     */
    override fun begin(parent: Entity.Tx): IndexTransaction = Tx(parent)

    /**
     * Closes this [VAPlusIndex] and the associated data structures.
     */
    override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }

    /**
     * A [IndexTransaction] that affects this [UniqueHashIndex].
     */
    private inner class Tx(parent: Entity.Tx) : Index.Tx(parent) {

        /**
         * (Re-)builds the [VAPlusIndex].
         */
        override fun rebuild() = this.localLock.read {
            checkValidForWrite()

            /* Clear existing map. */
            this@VAPlusIndex.signatures.clear()

            /* VAPlus. */
            val vaPlus = VAPlus()
            val trainingSize = 1000
            val minimumNumberOfTuples = 1000
            val dimension = this.columns[0].logicalSize

            // VA-file get data sample
            val dataSampleTmp = vaPlus.getDataSample(this.parent, this.columns, maxOf(trainingSize, minimumNumberOfTuples))

            // VA-file in KLT domain
            val (dataSample, kltMatrix) = vaPlus.transformToKLTDomain(dataSampleTmp)

            // Non-uniform bit allocation
            val b = vaPlus.nonUniformBitAllocation(dataSample, dimension * 2)
            val signatureGenerator = SignatureGenerator(b)

            // Non-uniform quantization
            val marks = vaPlus.nonUniformQuantization(dataSample, b)

            // Indexing
            val kltMatrixBar = kltMatrix.transpose()
            this.parent.scan(this@VAPlusIndex.columns).forEach { record ->
                val doubleArray = vaPlus.convertToDoubleArray(record[this.columns[0]] as VectorValue<*>)
                val dataMatrix = MatrixUtils.createRealMatrix(arrayOf(doubleArray))
                val vector = kltMatrixBar.multiply(dataMatrix.transpose()).getColumnVector(0).toArray()
                //val signature = signatureGenerator.toSignature(vaPlus.getCells(vector, marks))
                val signature = vaPlus.getCells(vector, marks)
                this@VAPlusIndex.signatures.add(VAPlusSignature(record.tupleId, signature))
            }
            val meta = VAPlusMeta(marks, signatureGenerator, kltMatrix)
            this@VAPlusIndex.meta.set(meta)
        }

        /**
         * Updates the [SuperBitLSHIndex] with the provided [DataChangeEvent]s. This method determines,
         * whether the [Record] affected by the [DataChangeEvent] should be added or updated
         *
         * @param update Collection of [DataChangeEvent]s to process.
         */
        override fun update(update: Collection<DataChangeEvent>) = this.localLock.read {
            checkValidForWrite()
            TODO()
        }

        /**
         * Performs a lookup through this [VAPlusIndex.Tx] and returns a [CloseableIterator] of
         * all [TupleId]s that match the [Predicate]. Only supports [AtomicBooleanPredicate]s.
         *
         * The [CloseableIterator] is not thread safe!
         *
         * <strong>Important:</strong> It remains to the caller to close the [CloseableIterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @param tx Reference to the [Entity.Tx] the call to this method belongs to.
         *
         * @return The resulting [CloseableIterator]
         */
        override fun filter(predicate: Predicate): CloseableIterator<Record> = object : CloseableIterator<Record> {

            private val predicate = if (predicate is KnnPredicate<*>) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@VAPlusIndex.name}' (vaf-index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /* Performs some sanity checks. */
            init {
                checkValidForRead()

                if (this.predicate.columns.first() == this@VAPlusIndex.columns[0]) {
                    throw QueryException.UnsupportedPredicateException("Index '${this@VAPlusIndex.name}' (vaf-index) does not support the provided predicate.")
                }
            }

            /** Generates a shared lock on the enclosing [Tx]. This lock is kept until the [CloseableIterator] is closed. */
            private val stamp = this@Tx.localLock.readLock()

            /** [VAPlus] instance . */
            private val vaPlus = VAPlus()

            /** Meta record with all the helper classes.*/
            private val meta = this@VAPlusIndex.meta.get()

            /* Prepare HeapSelect data structures for Phase 1KNN query.*/
            val heapsP1 = Array<MinHeapSelection<ComparablePair<Pair<Long, DoubleValue>, DoubleValue>>>(this.predicate.query.size) {
                MinHeapSelection(5 * this.predicate.k)
            }

            /* Prepare HeapSelect data structures for Phase 2 of KNN query.*/
            val heapsP2 = Array<MinHeapSelection<ComparablePair<Long, DoubleValue>>>(this.predicate.query.size) {
                MinHeapSelection(this.predicate.k)
            }

            /* Prepare empty IntArray for bounds estimation. */
            val d = IntArray(this.predicate.query.size) {
                Int.MAX_VALUE
            }

            /* Calculate the relevant bounds per query. */
            val queryBounds = this.predicate.query.map {
                /* Transform into KLT domain. */
                val dataMatrix = MatrixUtils.createRealMatrix(arrayOf(vaPlus.convertToDoubleArray(it)))
                val vector = meta.kltMatrix.multiply(dataMatrix.transpose()).getColumnVector(0).toArray()

                val bounds = vaPlus.computeBounds(vector, meta.marks)
                val (lbIndex, lbBounds) = vaPlus.compressBounds(bounds.first)
                val (ubIndex, ubBounds) = vaPlus.compressBounds(bounds.second)

                Pair(Pair(lbIndex, lbBounds), Pair(ubIndex, ubBounds))
            }

            /** Flag indicating whether this [CloseableIterator] is open and ready for use. */
            @Volatile
            override var isOpen = true
                private set

            override fun hasNext(): Boolean {
                check(this.isOpen) { "Illegal invocation of hasNext(): This CloseableIterator has been closed." }
                TODO("Not yet implemented")
            }

            override fun next(): Record {
                check(this.isOpen) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                TODO("Not yet implemented")
            }

            override fun close() {
                if (!this.isOpen) {
                    this@Tx.localLock.unlock(this.stamp)
                    this.isOpen = false
                }
            }
        }

        /** Performs the actual COMMIT operation by rolling back the [DB]. */
        override fun performCommit() {
            this@VAPlusIndex.db.commit()
        }

        /** Performs the actual ROLLBACK operation by rolling back the [DB]. */
        override fun performRollback() {
            this@VAPlusIndex.db.rollback()
        }

        override fun cleanup() {
            /* No Op. */
        }
    }
}