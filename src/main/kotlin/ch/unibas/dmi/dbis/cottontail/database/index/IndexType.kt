package ch.unibas.dmi.dbis.cottontail.database.index

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.index.hash.UniqueHashIndex
import ch.unibas.dmi.dbis.cottontail.database.index.lucene.LuceneIndex
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef

internal enum class IndexType {
    HASH_UQ, /* A hash based index with unique values. */
    HASH, /* A hash based index. */
    BTREE, /* A BTree based index. */
    LUCENE, /* A Lucene based index (fulltext search). */
    VAF, /* A VA file based index (for exact kNN lookup). */
    PQ, /* A product quantization  based index (for approximate kNN lookup). */
    SH; /* A spectral hashing  based index (for approximate kNN lookup). */

    /**
     * Opens an index of this [IndexType] using the given name and [Entity].
     *
     * @param name Name of the [Index]
     * @param entity The [Entity] the desired [Index] belongs to.
     */
    fun open(name: String, entity: Entity, columns: Array<ColumnDef<*>>): Index = when(this) {
        HASH_UQ -> UniqueHashIndex(name, entity, columns)
        LUCENE -> LuceneIndex(name, entity, columns)
        else -> TODO()
    }

    /**
     * Creates an index of this [IndexType] using the given name and [Entity].
     *
     * @param name Name of the [Index]
     * @param entity The [Entity] the desired [Index] belongs to.
     * @param columns The [ColumnDef] for which to create the [Index]
     * @param params Additions configuration params.
     */
    fun create(name: String, entity: Entity, columns: Array<ColumnDef<*>>, params: Map<String,String> = emptyMap()) = when (this) {
        HASH_UQ -> UniqueHashIndex(name, entity, columns)
        LUCENE -> LuceneIndex(name, entity, columns)
        else -> TODO()
    }
}