package org.vitrivr.cottontail.server.grpc.helper

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.predicates.KnnPredicateHint
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.Name

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.Entity].
 *
 * @return [Name.SchemaName] for the given [CottontailGrpc.Schema]
 */
fun CottontailGrpc.Schema.fqn(): Name.SchemaName = Name.SchemaName(this.name)

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.Entity].
 *
 * @return [Name.EntityName] for the given [CottontailGrpc.Entity]
 */
fun CottontailGrpc.Entity.fqn(): Name.EntityName = Name.EntityName(this.schema.name, this.name)

/**
 * Extension function that generates the FQN for the given [CottontailGrpc.Entity].
 *
 * @return [Name.IndexName] for the given [CottontailGrpc.Index]
 */
fun CottontailGrpc.Index.fqn(): Name.IndexName = Name.IndexName(this.entity.schema.name, this.entity.name, this.name)

/**
 * Extension function that generates the [CottontailGrpc.Entity] for the given [Name.EntityName].
 *
 * @return [CottontailGrpc.Entity] for the given [Name.EntityName].
 */
fun Name.EntityName.proto(): CottontailGrpc.Entity = CottontailGrpc.Entity.newBuilder().setName(this.simple).setSchema(this.schema().proto()).build()

/**
 * Extension function that generates the [CottontailGrpc.Schema] for the given [Name.SchemaName].
 *
 * @return [CottontailGrpc.Schema] for the given [Name.SchemaName].
 */
fun Name.SchemaName.proto() = CottontailGrpc.Schema.newBuilder().setName(this.simple).build()

/**
 * Extension function that generates the [CottontailGrpc.From] for the given [Name.EntityName].
 *
 * @return [CottontailGrpc.Schema] for the given [Name.SchemaName].
 */
fun Name.EntityName.protoFrom(): CottontailGrpc.From = CottontailGrpc.From.newBuilder().setEntity(this.proto()).build()

/**
 * Extension function that generates the [KnnPredicateHint] for the given [CottontailGrpc.KnnHint].
 *
 * @param entity The [Entity] the given [KnnPredicateHint] is evaluated for.
 * @return Fully qualified name for the given [KnnPredicateHint]
 */
fun CottontailGrpc.KnnHint.toHint(entity: Entity): KnnPredicateHint? = when (this.hintCase) {
    CottontailGrpc.KnnHint.HintCase.ALLOWINEXACTINDEXHINT -> KnnPredicateHint.AllowInexactKnnPredicateHint(this.allowInexactIndexHint.allow)
    CottontailGrpc.KnnHint.HintCase.NOINDEXHINT -> KnnPredicateHint.NoIndexKnnPredicateHint
    CottontailGrpc.KnnHint.HintCase.TYPEINDEXHINT -> KnnPredicateHint.IndexTypeKnnPredicateHint(IndexType.valueOf(this.typeIndexHint.type.toString()))
    CottontailGrpc.KnnHint.HintCase.NAMEINDEXHINT -> KnnPredicateHint.IndexNameKnnPredicateHint(entity.name.index(this.nameIndexHint.name), this.nameIndexHint.parametersMap)
    CottontailGrpc.KnnHint.HintCase.PARALLELINDEXHINT -> KnnPredicateHint.ParallelKnnPredicateHint(this.parallelIndexHint.min, this.parallelIndexHint.max)
    else -> null
}