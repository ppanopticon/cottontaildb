package org.vitrivr.cottontail.server.grpc.helper

import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.components.*
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.DeleteLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.UpdateLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.KnnLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.LimitLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.ProjectionLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntitySampleLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.ProjectionPhysicalNodeExpression
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.math.knn.metrics.Distances
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.values.*

/**
 * This helper class parses and binds queries issued through the gRPC endpoint. The process encompasses three steps:
 *
 * 1) The [CottontailGrpc.Query] is decomposed into its components.
 * 2) The gRPC query components are bound to Cottontail DB objects and internal query objects are constructed. This step includes some basic validation.
 * 3) A [LogicalNodeExpression] tree is constructed from the internal query objects.
 *
 * @author Ralph Gasser
 * @version 1.2.2
 */
class GrpcQueryBinder(val catalogue: Catalogue) {
    /**
     * Binds the given [CottontailGrpc.Query] to the database objects and thereby creates a tree of [LogicalNodeExpression]s.
     *
     * @param query The [CottontailGrpc.Query] that should be bound.
     * @return [LogicalNodeExpression]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun parseAndBind(query: CottontailGrpc.Query): LogicalNodeExpression {
        if (!query.hasFrom()) throw QueryException.QuerySyntaxException("Missing FROM-clause in query.")
        if (query.from.hasQuery()) {
            throw QueryException.QuerySyntaxException("Cottontail DB currently doesn't support sub-selects.")
        }
        return parseAndBindSimpleQuery(query)
    }

    /**
     * Binds the given [CottontailGrpc.UpdateMessage] to the database objects and thereby creates
     * a tree of [LogicalNodeExpression]s.
     *
     * @param update The [CottontailGrpc.UpdateMessage] that should be bound.
     * @return [LogicalNodeExpression]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun parseAndBindUpdate(update: CottontailGrpc.UpdateMessage): LogicalNodeExpression {
        /* Create SCAN-clause. */
        val scanClause = parseAndBindSimpleFrom(update.from)
        val entity: Entity = scanClause.first
        var root: LogicalNodeExpression = scanClause.second

        /* Create WHERE-clause. */
        if (update.hasWhere()) {
            val where = FilterLogicalNodeExpression(parseAndBindBooleanPredicate(entity, update.where))
            where.addInput(root)
            root = where
        }

        /* Parse values to update. */
        val values = update.tuple.dataMap.map {
            val columnName = entity.name.column(it.key)
            val column = entity.columnForName(columnName)
                    ?: throw QueryException.QueryBindException("Failed to bind column '$columnName'. Column does not exist on entity'.")
            column to it.value.toValue(column)
        }

        /* Create and return UPDATE-clause. */
        val upd = UpdateLogicalNodeExpression(entity, values)
        upd.addInput(root)
        return upd
    }

    /**
     * Binds the given [CottontailGrpc.DeleteMessage] to the database objects and thereby creates
     * a tree of [LogicalNodeExpression]s.
     *
     * @param delete The [CottontailGrpc.DeleteMessage] that should be bound.
     * @return [LogicalNodeExpression]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun parseAndBindDelete(delete: CottontailGrpc.DeleteMessage): LogicalNodeExpression {
        /* Create SCAN-clause. */
        val scanClause = parseAndBindSimpleFrom(delete.from)
        val entity: Entity = scanClause.first
        var root: LogicalNodeExpression = scanClause.second

        /* Create WHERE-clause. */
        if (delete.hasWhere()) {
            val where = FilterLogicalNodeExpression(parseAndBindBooleanPredicate(entity, delete.where))
            where.addInput(root)
            root = where
        }

        /* Create and return DELETE-clause. */
        val del = DeleteLogicalNodeExpression(entity)
        del.addInput(root)
        return del
    }

    /**
     * Binds the given [CottontailGrpc.TruncateMessage] to the database objects and thereby creates
     * a tree of [LogicalNodeExpression]s.
     *
     * @param truncate The [CottontailGrpc.DeleteMessage] that should be bound.
     * @return [LogicalNodeExpression]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun parseAndBindTruncate(truncate: CottontailGrpc.TruncateMessage): LogicalNodeExpression {
        /* Create SCAN-clause. */
        val scanClause = parseAndBindSimpleFrom(truncate.from)
        val entity: Entity = scanClause.first
        val root: LogicalNodeExpression = scanClause.second

        /* Create and return DELETE-clause. */
        val del = DeleteLogicalNodeExpression(entity)
        del.addInput(root)
        return del
    }

    /**
     * Parses and binds a simple [CottontailGrpc.Query] without any joins and subselects and thereby
     * generates a tree of [LogicalNodeExpression]s.
     *
     * @param query The simple [CottontailGrpc.Query] object.
     */
    private fun parseAndBindSimpleQuery(query: CottontailGrpc.Query): LogicalNodeExpression {
        /* Create scan clause. */
        val scanClause = parseAndBindSimpleFrom(query.from)
        val entity: Entity = scanClause.first
        var root: LogicalNodeExpression = scanClause.second

        /* Create WHERE-clause. */
        if (query.hasWhere()) {
            val where = FilterLogicalNodeExpression(parseAndBindBooleanPredicate(entity, query.where))
            where.addInput(root)
            root = where
        }

        /* Process kNN-clause (Important: mind precedence of WHERE-clause. */
        if (query.hasKnn()) {
            val knn = KnnLogicalNodeExpression(parseAndBindKnnPredicate(entity, query.knn))
            knn.addInput(root)
            root = knn
        }

        /* Process projection clause. */
        root = if (query.hasProjection()) {
            val projection = parseAndBindProjection(entity, query.projection)
            projection.addInput(root)
            projection
        } else {
            val projection = parseAndBindProjection(entity, CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.Operation.SELECT).putAttributes("*", "").build())
            projection.addInput(root)
            projection
        }

        /* Process LIMIT and SKIP. */
        if (query.limit > 0L || query.skip > 0L) {
            val limit = LimitLogicalNodeExpression(query.limit, query.skip)
            limit.addInput(root)
            root = limit
        }

        return root
    }

    /**
     * Parses and binds a [CottontailGrpc.From] clause.
     *
     * @param from The [CottontailGrpc.From] object.
     * @return The resulting [Pair] of [Entity] and [NullaryLogicalNodeExpression].
     */
    private fun parseAndBindSimpleFrom(from: CottontailGrpc.From): Pair<Entity,NullaryLogicalNodeExpression> = try {
        when (from.fromCase) {
            CottontailGrpc.From.FromCase.ENTITY -> {
                val entityName = from.entity.fqn()
                val entity = this.catalogue.instantiateEntity(entityName)
                Pair(entity, EntityScanLogicalNodeExpression(entity = entity))
            }
            CottontailGrpc.From.FromCase.SAMPLE -> {
                val entityName = from.sample.entity.fqn()
                val entity = this.catalogue.instantiateEntity(entityName)
                Pair(entity, EntitySampleLogicalNodeExpression(entity = entity, size = from.sample.size, seed = from.sample.seed))
            }
            else -> throw QueryException.QuerySyntaxException("Invalid FROM-clause in query.")
        }
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${from.entity.fqn()}'. Schema does not exist!")
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${from.entity.fqn()}'. Entity does not exist!")
    } catch (e: DatabaseException) {
        throw QueryException.QueryBindException("Failed to bind ${from.entity.fqn()}. Database error!")
    }


    /**
     * Parses and binds a [CottontailGrpc.Where] clause.
     *
     * @param entity The [Entity] from which fetch columns.
     * @param where The [CottontailGrpc.Where] object.
     *
     * @return The resulting [AtomicBooleanPredicate].
     */
    private fun parseAndBindBooleanPredicate(entity: Entity, where: CottontailGrpc.Where): BooleanPredicate = when (where.predicateCase) {
        CottontailGrpc.Where.PredicateCase.ATOMIC -> parseAndBindAtomicBooleanPredicate(entity, where.atomic)
        CottontailGrpc.Where.PredicateCase.COMPOUND -> parseAndBindCompoundBooleanPredicate(entity, where.compound)
        CottontailGrpc.Where.PredicateCase.PREDICATE_NOT_SET -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
        null -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
    }

    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param entity The [Entity] from which fetch columns.
     * @param compound The [CottontailGrpc.CompoundBooleanPredicate] object.
     *
     * @return The resulting [AtomicBooleanPredicate].
     */
    private fun parseAndBindCompoundBooleanPredicate(entity: Entity, compound: CottontailGrpc.CompoundBooleanPredicate): CompoundBooleanPredicate {
        val left = when (compound.leftCase) {
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.ALEFT -> parseAndBindAtomicBooleanPredicate(entity, compound.aleft)
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.CLEFT -> parseAndBindCompoundBooleanPredicate(entity, compound.cleft)
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.LEFT_NOT_SET -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
            null -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
        }

        val right = when (compound.rightCase) {
            CottontailGrpc.CompoundBooleanPredicate.RightCase.ARIGHT -> parseAndBindAtomicBooleanPredicate(entity, compound.aright)
            CottontailGrpc.CompoundBooleanPredicate.RightCase.CRIGHT -> parseAndBindCompoundBooleanPredicate(entity, compound.cright)
            CottontailGrpc.CompoundBooleanPredicate.RightCase.RIGHT_NOT_SET -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
            null -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
        }

        return try {
            CompoundBooleanPredicate(ConnectionOperator.valueOf(compound.op.name), left, right)
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("'${compound.op.name}' is not a valid connection operator for a boolean predicate!")
        }
    }

    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param entity The [Entity] from which fetch columns.
     * @param atomic The [CottontailGrpc.AtomicLiteralBooleanPredicate] object.
     *
     * @return The resulting [AtomicBooleanPredicate].
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseAndBindAtomicBooleanPredicate(entity: Entity, atomic: CottontailGrpc.AtomicLiteralBooleanPredicate): AtomicBooleanPredicate<*> {
        val columnName = entity.name.column(atomic.attribute)
        val column = entity.columnForName(columnName) ?: throw QueryException.QueryBindException("Failed to bind column '$columnName'. Column does not exist on entity'.")
        val operator = try {
            ComparisonOperator.valueOf(atomic.op.name)
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("'${atomic.op.name}' is not a valid comparison operator for a boolean predicate!")
        }

        /* Perform some sanity checks. */
        when {
            operator == ComparisonOperator.LIKE && !entity.hasIndexForColumn(column, IndexType.LUCENE) -> throw QueryException.QueryBindException("Failed to bind clause '${atomic.attribute} LIKE :1' on entity '${entity.name}'. The entity does not have a text-index on the specified column '${column.name}', which is required for LIKE comparisons.")
        }

        /* Return the resulting AtomicBooleanPredicate. */
        return AtomicBooleanPredicate(column, operator, atomic.not, atomic.dataList.map {
            it.toValue(column)
                    ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.")
        })
    }

    /**
     * Parses and binds the kNN-lookup part of a GRPC [CottontailGrpc.Query]
     *
     * @param entity The [Entity] from which fetch columns.
     * @param knn The [CottontailGrpc.Knn] object.
     *
     * @return The resulting [KnnPredicate].
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseAndBindKnnPredicate(entity: Entity, knn: CottontailGrpc.Knn): KnnPredicate<*> {
        val columnName = entity.name.column(knn.attribute)
        val column = entity.columnForName(columnName)
                ?: throw QueryException.QueryBindException("Failed to bind column '$columnName'. Column does not exist on entity!")
        val distance = Distances.valueOf(knn.distance.name).kernel
        val hint = knn.hint.toHint(entity)


        return when (column.type) {
            is DoubleVectorColumnType -> {
                val query = knn.queryList.map { q -> q.toDoubleVectorValue() }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { w -> w.toDoubleVectorValue() }
                    if (weights.all { it.allOnes() }) {
                        KnnPredicate(column = column as ColumnDef<DoubleVectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                    } else {
                        KnnPredicate(column = column as ColumnDef<DoubleVectorValue>, k = knn.k, query = query, weights = weights, distance = distance, hint = hint)
                    }
                } else {
                    KnnPredicate(column = column as ColumnDef<DoubleVectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                }
            }
            is FloatVectorColumnType -> {
                val query = knn.queryList.map { q -> q.toFloatVectorValue() }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { it.toFloatVectorValue() }
                    if (weights.all { it.allOnes() }) {
                        KnnPredicate(column = column as ColumnDef<FloatVectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                    } else {
                        KnnPredicate(column = column as ColumnDef<FloatVectorValue>, k = knn.k, query = query, weights = weights, distance = distance, hint = hint)
                    }
                } else {
                    KnnPredicate(column = column as ColumnDef<FloatVectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                }
            }
            is LongVectorColumnType -> {
                val query = knn.queryList.map { q -> q.toLongVectorValue() }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { it.toLongVectorValue() }
                    if (weights.all { it.allOnes() }) {
                        KnnPredicate(column = column as ColumnDef<LongVectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                    } else {
                        KnnPredicate(column = column as ColumnDef<LongVectorValue>, k = knn.k, query = query, weights = weights, distance = distance, hint = hint)
                    }
                } else {
                    KnnPredicate(column = column as ColumnDef<LongVectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                }
            }
            is IntVectorColumnType -> {
                val query = knn.queryList.map { q -> q.toIntVectorValue() }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { it.toIntVectorValue() }
                    if (weights.all { it.allOnes() }) {
                        KnnPredicate(column = column as ColumnDef<IntVectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                    } else {
                        KnnPredicate(column = column as ColumnDef<IntVectorValue>, k = knn.k, query = query, weights = weights, distance = distance, hint = hint)
                    }
                } else {
                    KnnPredicate(column = column as ColumnDef<IntVectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                }
            }
            is BooleanVectorColumnType -> {
                val query = knn.queryList.map { q -> q.toBooleanVectorValue() }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { it.toBooleanVectorValue() }
                    if (weights.all { it.allOnes() }) {
                        KnnPredicate(column = column as ColumnDef<BooleanVectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                    } else {
                        KnnPredicate(column = column as ColumnDef<BooleanVectorValue>, k = knn.k, query = query, weights = weights, distance = distance, hint = hint)
                    }
                } else {
                    KnnPredicate(column = column as ColumnDef<BooleanVectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                }
            }
            is Complex32VectorColumnType -> {
                val query = knn.queryList.map { q -> q.toComplex32VectorValue() }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { it.toComplex32VectorValue() }
                    if (weights.all { it.allOnes() }) {
                        KnnPredicate(column = column as ColumnDef<Complex32VectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                    } else {
                        KnnPredicate(column = column as ColumnDef<Complex32VectorValue>, k = knn.k, query = query, weights = weights, distance = distance, hint = hint)
                    }
                } else {
                    KnnPredicate(column = column as ColumnDef<Complex32VectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                }
            }
            is Complex64VectorColumnType -> {
                val query = knn.queryList.map { q -> q.toComplex64VectorValue() }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { it.toComplex64VectorValue() }
                    if (weights.all { it.allOnes() }) {
                        KnnPredicate(column = column as ColumnDef<Complex64VectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                    } else {
                        KnnPredicate(column = column as ColumnDef<Complex64VectorValue>, k = knn.k, query = query, weights = weights, distance = distance, hint = hint)
                    }
                } else{
                    KnnPredicate(column = column as ColumnDef<Complex64VectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                }
            }
            else -> throw QueryException.QuerySyntaxException("A kNN predicate does not contain a valid query vector!")
        }
    }

    /**
     * Parses and binds the projection part of a gRPC [CottontailGrpc.Query]
     *
     * @param projection The [CottontailGrpc.Projection] object.
     *
     * @return The resulting [ProjectionPhysicalNodeExpression].
     */
    private fun parseAndBindProjection(entity: Entity, projection: CottontailGrpc.Projection): ProjectionLogicalNodeExpression = try {
        /* Handle other kinds of projections. */
        val fields = mutableListOf<Pair<Name.ColumnName, Name.ColumnName?>>()
        projection.attributesMap.forEach { (expr, alias) ->
            val split = expr.split('.')
            when (split.size) {
                1 -> {
                    val columnName = entity.name.column(split[0])
                    if (columnName.wildcard) {
                        fields.add(Pair(columnName, null as Name.ColumnName?))
                    } else {
                        fields.add(Pair(columnName, if (alias.isBlank()) {
                            null
                        } else {
                            Name.ColumnName(alias)
                        }))
                    }
                }
                2 -> {
                    if (split[0] == entity.name.simple) {
                        val columnName = entity.name.column(split[1])
                        if (columnName.wildcard) {
                            fields.add(Pair(columnName, null as Name.ColumnName?))
                        } else {
                            fields.add(Pair(columnName, if (alias.isBlank()) {
                                null
                            } else {
                                Name.ColumnName(alias)
                            }))
                        }
                    }
                }
                3 -> {
                    if (split[0] == entity.name.schema().simple && split[1] == entity.name.simple) {
                        val columnName = entity.name.column(split[2])
                        if (columnName.wildcard) {
                            fields.add(Pair(columnName, null as Name.ColumnName?))
                        } else {
                            fields.add(Pair(columnName, if (alias.isBlank()) {
                                null
                            } else {
                                Name.ColumnName(alias)
                            }))
                        }
                    }
                }
                else -> throw QueryException.QuerySyntaxException("The provided column '$expr' is invalid.")
            }
        }
        ProjectionLogicalNodeExpression(type = Projection.valueOf(projection.op.name), fields = fields)
    } catch (e: java.lang.IllegalArgumentException) {
        throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): ${projection.op} is not supported.")
    }
}