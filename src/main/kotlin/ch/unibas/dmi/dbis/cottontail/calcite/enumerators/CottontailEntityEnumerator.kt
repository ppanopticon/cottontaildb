package ch.unibas.dmi.dbis.cottontail.calcite.enumerators

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.values.Value

import org.apache.calcite.linq4j.Enumerator

import java.util.concurrent.atomic.AtomicBoolean

/**
 * This is the default [Enumerator] implementation used for full table scans over a given Cottontail DB [Entity]. It allows
 * for projection operations to be pushed down to the execution engine, since Cottontail DB is a column store.
 *
 * The implementation of [CottontailEntityEnumerator] is NOT thread safe due to the nature of the [Enumerator] interface. If
 * multiple  threads access an instance, access must be synchronized.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class CottontailEntityEnumerator (entity: Entity, fields: Array<String>, private val cancelFlag: AtomicBoolean) : Enumerator<Array<Any?>> {

    /** Fields that should be scanned by this [CottontailEntityEnumerator]. */
    private val fields = fields.map { entity.columnForName(it) ?: throw QueryException.QueryBindException("The field $it does not exist on entity ${entity.fqn}.")}.toTypedArray()

    /** Fields that should be scanned by this [CottontailEntityEnumerator]. */
    private val tx = entity.Tx(readonly = true, columns = this.fields)

    /** Snapshot of the tuple IDs held by this [Entity]. Required for iteration. */
    private val cursor = this.tx.listTupleIds()

    /** A flag indicating whether this [CottontailEntityEnumerator] has been closed. */
    private val closed = AtomicBoolean(false)

    /** Internal cache to prevent repeated I/O to the same row. */
    @Volatile
    private var cached: Array<Any?>? = null

    /** The pointer to the */
    @Volatile
    var pointer = Enumerators.BOF_FLAG
        private set

    /**
     * Moves the pointer of this [CottontailEntityEnumerator] to the next record, if there are records left to be read.
     * Returns true on success, and false otherwise.
     *
     * @return true, if pointer was moved successfully, false otherwise.
     */
    override fun moveNext(): Boolean {
        if (this.cancelFlag.get() || this.closed.get()) {
            return false
        }
        return if (this.cursor.hasNext()) {
            this.pointer = this.cursor.next()
            this.cached = null
            true
        } else {
            this.pointer = Enumerators.EOF_FLAG
            this.cached = null
            false
        }
    }

    /**
     * Returns the value held by this [CottontailEntityEnumerator] at the current pointer position. For a given position of the pointer,
     * this method can be called multiple times. The retrieved value will be cached between repeated invocations of this method.
     *
     * @return The value at the current pointer.
     */
    override fun current(): Array<Any?> {
        if (this.pointer == Enumerators.EOF_FLAG || this.pointer == Enumerators.BOF_FLAG) {
            throw NoSuchElementException()
        }
        if (cached == null) {
            cached = this.tx.read(pointer).values.map { it?.value }.toTypedArray()
        }
        return this.cached!!
    }

    /**
     * Closes this [CottontailEntityEnumerator]. This method is idempotent.
     */
    override fun close() {
        if (!this.closed.get()) {
            this.closed.set(true)
            this.cached = null
            this.tx.close()
        }
    }

    /**
     * Resetting a [CottontailEntityEnumerator] is not supported.
     */
    override fun reset() {
        throw UnsupportedOperationException()
    }
}