package org.vitrivr.cottontail.server.grpc.helper

import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.components.*
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.DeleteLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.InsertLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.UpdateLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.KnnLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.LimitLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.ProjectionLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntitySampleLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntitySourceLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.RecordSourceLogicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.ProjectionPhysicalNodeExpression
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.math.knn.metrics.Distances
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.pattern.LikePatternValue
import org.vitrivr.cottontail.model.values.pattern.LucenePatternValue

/**
 * This helper class parses and binds queries issued through the gRPC endpoint. The process encompasses three steps:
 *
 * 1) The [CottontailGrpc.Query] is decomposed into its components.
 * 2) The gRPC query components are bound to Cottontail DB objects and internal query objects are constructed. This step includes some basic validation.
 * 3) A [LogicalNodeExpression] tree is constructed from the internal query objects.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class GrpcQueryBinder(val catalogue: Catalogue) {

    companion object {
        val DEFAULT_PROJECTION = CottontailGrpc.Projection.newBuilder()
                .setOp(CottontailGrpc.Projection.ProjectionOperation.SELECT)
                .addColumns(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("*")))
                .build()
    }

    /**
     * Binds the given [CottontailGrpc.Query] to the database objects and thereby creates a tree of [LogicalNodeExpression]s.
     *
     * @param query The [CottontailGrpc.Query] that should be bound.
     * @return [LogicalNodeExpression]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun parseAndBindQuery(query: CottontailGrpc.Query, context: TransactionContext): LogicalNodeExpression {
        if (!query.hasFrom()) throw QueryException.QuerySyntaxException("Missing FROM-clause in query.")
        if (query.from.hasQuery()) {
            throw QueryException.QuerySyntaxException("Cottontail DB currently doesn't support sub-selects.")
        }
        return parseAndBindSimpleQuery(query, context)
    }

    /**
     * Binds the given [CottontailGrpc.InsertMessage] to the database objects and thereby creates
     * a tree of [LogicalNodeExpression]s.
     *
     * @param insert The [ CottontailGrpc.InsertMessage] that should be bound.
     * @param context The [TransactionContext] to use for querying the [Catalogue]
     * @return [LogicalNodeExpression]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun parseAndBindInsert(insert: CottontailGrpc.InsertMessage, context: TransactionContext): LogicalNodeExpression = try {
        /* Parse entity for INSERT. */
        val entity = parseAndBindEntity(insert.from.entity, context)

        /* Obtain transaction for entity. */
        val entityTx = context.getTx(entity) as EntityTx

        /* Parse columns to INSERT. */
        val columns = Array(insert.insertsCount) {
            val columnName = insert.insertsList[it].column.fqn()
            entityTx.columnForName(columnName).columnDef
        }
        val root: LogicalNodeExpression = RecordSourceLogicalNodeExpression(columns)

        /* Create and return INSERT-clause. */
        val ins = InsertLogicalNodeExpression(entity)
        ins.addInput(root)
        ins
    } catch (e: DatabaseException.ColumnDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
    }

    /**
     * Binds the given [CottontailGrpc.UpdateMessage] to the database objects and thereby creates
     * a tree of [LogicalNodeExpression]s.
     *
     * @param update The [CottontailGrpc.UpdateMessage] that should be bound.
     * @param context The [TransactionContext] to use for querying the [Catalogue]
     * @return [LogicalNodeExpression]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun parseAndBindUpdate(update: CottontailGrpc.UpdateMessage, context: TransactionContext): LogicalNodeExpression = try {
        /* Create SCAN-clause. */
        val scanClause = parseAndBindSimpleFrom(update.from, context)
        val entity: Entity = scanClause.entity
        var root: LogicalNodeExpression = scanClause

        /* Create WHERE-clause. */
        if (update.hasWhere()) {
            val where = FilterLogicalNodeExpression(parseAndBindBooleanPredicate(entity, update.where, context))
            where.addInput(root)
            root = where
        }

        /* Obtain transaction for entity. */
        val entityTx = context.getTx(entity) as EntityTx

        /* Parse values to update. */
        val values = update.updatesList.map {
            val column = entityTx.columnForName(it.column.fqn()).columnDef
            column to it.value.toValue(column)
        }

        /* Create and return UPDATE-clause. */
        val upd = UpdateLogicalNodeExpression(entity, values)
        upd.addInput(root)
        upd
    } catch (e: DatabaseException.ColumnDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
    }

    /**
     * Binds the given [CottontailGrpc.DeleteMessage] to the database objects and thereby creates
     * a tree of [LogicalNodeExpression]s.
     *
     * @param delete The [CottontailGrpc.DeleteMessage] that should be bound.
     * @param context The [TransactionContext] to use for querying the [Catalogue]
     * @return [LogicalNodeExpression]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun parseAndBindDelete(delete: CottontailGrpc.DeleteMessage, context: TransactionContext): LogicalNodeExpression {
        /* Create SCAN-clause. */
        val scanClause = parseAndBindSimpleFrom(delete.from, context)
        val entity: Entity = scanClause.entity
        var root: LogicalNodeExpression = scanClause

        /* Create WHERE-clause. */
        if (delete.hasWhere()) {
            val where = FilterLogicalNodeExpression(parseAndBindBooleanPredicate(entity, delete.where, context))
            where.addInput(root)
            root = where
        }

        /* Create and return DELETE-clause. */
        val del = DeleteLogicalNodeExpression(entity)
        del.addInput(root)
        return del
    }

    /**
     * Parses and binds a simple [CottontailGrpc.Query] without any joins and sub-selects and thereby
     * generates a tree of [LogicalNodeExpression]s.
     *
     * @param query The simple [CottontailGrpc.Query] object.
     * @param context The [TransactionContext] to use for querying the [Catalogue]
     * @return [LogicalNodeExpression]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun parseAndBindSimpleQuery(query: CottontailGrpc.Query, context: TransactionContext): LogicalNodeExpression {
        /* Create scan clause. */
        val scanClause = parseAndBindSimpleFrom(query.from, context)
        val entity: Entity = scanClause.entity
        var root: LogicalNodeExpression = scanClause

        /* Create WHERE-clause. */
        if (query.hasWhere()) {
            val where = FilterLogicalNodeExpression(parseAndBindBooleanPredicate(entity, query.where, context))
            where.addInput(root)
            root = where
        }

        /* Process kNN-clause (Important: mind precedence of WHERE-clause. */
        if (query.hasKnn()) {
            val knn = KnnLogicalNodeExpression(parseAndBindKnnPredicate(entity, query.knn, context))
            knn.addInput(root)
            root = knn
        }

        /* Process projection clause. */
        root = if (query.hasProjection()) {
            val projection = parseAndBindProjection(entity, query.projection)
            projection.addInput(root)
            projection
        } else {

            val projection = parseAndBindProjection(entity, DEFAULT_PROJECTION)
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
     * Parses the given [CottontailGrpc.EntityName] and returns the corresponding [Entity].
     *
     * @param entity [CottontailGrpc.EntityName] to parse.
     * @param context The [TransactionContext] to use for querying the [Catalogue]
     * @return [Entity] that matches [CottontailGrpc.EntityName]
     */
    private fun parseAndBindEntity(entity: CottontailGrpc.EntityName, context: TransactionContext): Entity = try {
        val name = entity.fqn()
        val catalogueTx = context.getTx(this.catalogue) as CatalogueTx
        val schemaTx = context.getTx(catalogueTx.schemaForName(name.schema())) as SchemaTx
        schemaTx.entityForName(name)
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${e.schema}'. Schema does not exist!")
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${e.entity}'. Entity does not exist!")
    }

    /**
     * Parses and binds a [CottontailGrpc.From] clause.
     *
     * @param from The [CottontailGrpc.From] object.
     * @param context The [TransactionContext] to use for querying the [Catalogue]
     * @return The resulting [Pair] of [Entity] and [NullaryLogicalNodeExpression].
     */
    private fun parseAndBindSimpleFrom(from: CottontailGrpc.From, context: TransactionContext): EntitySourceLogicalNodeExpression = try {
        when (from.fromCase) {
            CottontailGrpc.From.FromCase.ENTITY -> EntityScanLogicalNodeExpression(entity = parseAndBindEntity(from.entity, context))
            CottontailGrpc.From.FromCase.SAMPLE -> EntitySampleLogicalNodeExpression(entity = parseAndBindEntity(from.entity, context), size = from.sample.size, seed = from.sample.seed)
            else -> throw QueryException.QuerySyntaxException("Invalid FROM-clause in query.")
        }
    } catch (e: DatabaseException) {
        throw QueryException.QueryBindException("Failed to bind ${from.entity.fqn()}. Database error!")
    }

    /**
     * Parses and binds a [CottontailGrpc.Where] clause.
     *
     * @param entity The [Entity] from which fetch columns.
     * @param where The [CottontailGrpc.Where] object.
     * @param context The [TransactionContext] to use for querying the [Catalogue]
     *
     * @return The resulting [AtomicBooleanPredicate].
     */
    private fun parseAndBindBooleanPredicate(entity: Entity, where: CottontailGrpc.Where, context: TransactionContext): BooleanPredicate = when (where.predicateCase) {
        CottontailGrpc.Where.PredicateCase.ATOMIC -> parseAndBindAtomicBooleanPredicate(entity, where.atomic, context)
        CottontailGrpc.Where.PredicateCase.COMPOUND -> parseAndBindCompoundBooleanPredicate(entity, where.compound, context)
        CottontailGrpc.Where.PredicateCase.PREDICATE_NOT_SET -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
        null -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
    }

    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param entity The [Entity] from which fetch columns.
     * @param compound The [CottontailGrpc.CompoundBooleanPredicate] object.
     * @param context The [TransactionContext] to use for querying the [Catalogue]
     *
     * @return The resulting [AtomicBooleanPredicate].
     */
    private fun parseAndBindCompoundBooleanPredicate(entity: Entity, compound: CottontailGrpc.CompoundBooleanPredicate, context: TransactionContext): CompoundBooleanPredicate {
        val left = when (compound.leftCase) {
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.ALEFT -> parseAndBindAtomicBooleanPredicate(entity, compound.aleft, context)
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.CLEFT -> parseAndBindCompoundBooleanPredicate(entity, compound.cleft, context)
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.LEFT_NOT_SET -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
            null -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
        }

        val right = when (compound.rightCase) {
            CottontailGrpc.CompoundBooleanPredicate.RightCase.ARIGHT -> parseAndBindAtomicBooleanPredicate(entity, compound.aright, context)
            CottontailGrpc.CompoundBooleanPredicate.RightCase.CRIGHT -> parseAndBindCompoundBooleanPredicate(entity, compound.cright, context)
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
     * @param context The [TransactionContext] to use for querying the [Catalogue]
     *
     * @return The resulting [AtomicBooleanPredicate].
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseAndBindAtomicBooleanPredicate(entity: Entity, atomic: CottontailGrpc.AtomicLiteralBooleanPredicate, context: TransactionContext): AtomicBooleanPredicate<*> = try {
        val columnName = atomic.left.fqn()
        val txn = context.getTx(entity) as EntityTx
        val column = txn.columnForName(columnName).columnDef
        val operator = try {
            ComparisonOperator.valueOf(atomic.op.name)
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("'${atomic.op.name}' is not a valid comparison operator for a boolean predicate!")
        }

        /* Return the resulting AtomicBooleanPredicate. */
        AtomicBooleanPredicate(column, operator, atomic.not, atomic.rightList.map {
            val v = it.toValue(column)
                    ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.")
            when (operator) {
                ComparisonOperator.LIKE -> {
                    if (v is StringValue) {
                        LikePatternValue(v.value)
                    } else {
                        throw QueryException.QuerySyntaxException("LIKE operator requires a parsable string value as second operand.")
                    }
                }
                ComparisonOperator.MATCH -> {
                    if (v is StringValue) {
                        LucenePatternValue(v.value)
                    } else {
                        throw QueryException.QuerySyntaxException("MATCH operator requires a parsable string value as second operand.")
                    }
                }
                else -> v
            }
        })
    } catch (e: DatabaseException.ColumnDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
    }

    /**
     * Parses and binds the kNN-lookup part of a GRPC [CottontailGrpc.Query]
     *
     * @param entity The [Entity] from which fetch columns.
     * @param knn The [CottontailGrpc.Knn] object.
     * @param context The [TransactionContext] to use for querying the [Catalogue]
     *s
     * @return The resulting [KnnPredicate].
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseAndBindKnnPredicate(entity: Entity, knn: CottontailGrpc.Knn, context: TransactionContext): KnnPredicate<*> = try {
        val columnName = knn.attribute.fqn()
        val txn = context.getTx(entity) as EntityTx
        val column = txn.columnForName(columnName).columnDef
        val distance = Distances.valueOf(knn.distance.name).kernel
        val hint = knn.hint.toHint(entity)

        when (column.type) {
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
                } else {
                    KnnPredicate(column = column as ColumnDef<Complex64VectorValue>, k = knn.k, query = query, distance = distance, hint = hint)
                }
            }
            else -> throw QueryException.QuerySyntaxException("A kNN predicate does not contain a valid query vector!")
        }
    } catch (e: DatabaseException.ColumnDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
    }

    /**
     * Parses and binds the projection part of a gRPC [CottontailGrpc.Query]
     *
     * @param projection The [CottontailGrpc.Projection] object.
     *
     * @return The resulting [ProjectionPhysicalNodeExpression].
     */
    private fun parseAndBindProjection(entity: Entity, projection: CottontailGrpc.Projection): ProjectionLogicalNodeExpression = try {
        val fields = projection.columnsList.map {
            val transmittedColumnName = it.column.fqn()
            val columnName = if (!transmittedColumnName.fqn) {
                entity.name.column(transmittedColumnName.simple)
            } else {
                transmittedColumnName
            }
            val alias = if (!columnName.wildcard && it.hasAlias()) {
                it.alias.fqn()
            } else {
                null
            }
            columnName to alias
        }
        ProjectionLogicalNodeExpression(type = Projection.valueOf(projection.op.name), fields = fields)
    } catch (e: java.lang.IllegalArgumentException) {
        throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): ${projection.op} is not supported.")
    }
}