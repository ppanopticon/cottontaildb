package org.vitrivr.cottontail.database.index

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.index.gg.GGIndex
import org.vitrivr.cottontail.database.index.gg.GGIndexConfig
import org.vitrivr.cottontail.database.index.hash.NonUniqueHashIndex
import org.vitrivr.cottontail.database.index.hash.UniqueHashIndex
import org.vitrivr.cottontail.database.index.lsh.superbit.SuperBitLSHIndex
import org.vitrivr.cottontail.database.index.lsh.superbit.SuperBitLSHIndexConfig
import org.vitrivr.cottontail.database.index.lucene.LuceneIndex
import org.vitrivr.cottontail.database.index.lucene.LuceneIndexConfig
import org.vitrivr.cottontail.database.index.pq.PQIndex
import org.vitrivr.cottontail.database.index.pq.PQIndexConfig
import org.vitrivr.cottontail.database.index.va.VAFIndex
import org.vitrivr.cottontail.database.index.va.VAFIndexConfig
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.VectorValue
import java.nio.file.Path

/**
 * A final list of types of [AbstractIndex] implementation.
 *
 * TODO: This could actually be more of a 'registry' type of facility, which would allow for extensions
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
enum class IndexType(val inexact: Boolean) {

    HASH_UQ(false), /* A hash based index with unique values. */

    HASH(false), /* A hash based index. */

    BTREE(false), /* A BTree based index. */

    LUCENE(false), /* A Lucene based index (fulltext search). */

    VAF(false), /* A VA file based index (for exact kNN lookup). */

    PQ(true), /* A product quantization based index (for approximate kNN lookup). */

    SH(true), /* A spectral hashing based index (for approximate kNN lookup). */

    LSH(true), /* A locality sensitive hashing based index for approximate kNN lookup with Lp distance. */

    LSH_SB(true), /* A super bit locality sensitive hashing based index for approximate kNN lookup with cosine distance. */

    GG(true);

    /**
     * Opens an index of this [IndexType] using the given name and [DefaultEntity].
     *
     * @param path [Name.IndexName] of the [AbstractIndex]
     * @param entity The [DefaultEntity] the desired [AbstractIndex] belongs to.
     */
    fun open(path: Path, entity: DefaultEntity): AbstractIndex = when (this) {
        HASH_UQ -> UniqueHashIndex(path, entity)
        HASH -> NonUniqueHashIndex(path, entity)
        LUCENE -> LuceneIndex(path, entity)
        LSH_SB -> SuperBitLSHIndex<VectorValue<*>>(path, entity)
        VAF -> VAFIndex(path, entity)
        PQ -> PQIndex(path, entity)
        GG -> GGIndex(path, entity)
        else -> throw NotImplementedError("Index of type $this is not implemented.")
    }

    /**
     * Creates an index of this [IndexType] using the given name and [DefaultEntity].
     *
     * @param name [Name.IndexName] of the [AbstractIndex]
     * @param entity The [DefaultEntity] the desired [AbstractIndex] belongs to.
     * @param columns The [ColumnDef] for which to create the [AbstractIndex]
     * @param params Additions configuration params.
     */
    fun create(
        path: Path,
        entity: DefaultEntity,
        name: Name.IndexName,
        columns: Array<ColumnDef<*>>,
        params: Map<String, String> = emptyMap()
    ): AbstractIndex {
        AbstractIndex.initialize(path, name, this, columns, entity.parent.parent.config)
        return when (this) {
            HASH_UQ -> UniqueHashIndex(path, entity)
            HASH -> NonUniqueHashIndex(path, entity)
            LUCENE -> LuceneIndex(path, entity, LuceneIndexConfig.fromParamMap(params))
            LSH_SB -> SuperBitLSHIndex<VectorValue<*>>(
                path,
                entity,
                SuperBitLSHIndexConfig.fromParamMap(params)
            )
            VAF -> VAFIndex(path, entity, VAFIndexConfig.fromParamMap(params))
            PQ -> PQIndex(path, entity, PQIndexConfig.fromParamMap(params))
            GG -> GGIndex(path, entity, GGIndexConfig.fromParamsMap(params))
            else -> throw NotImplementedError("Index of type $this is not implemented.")
        }
    }
}