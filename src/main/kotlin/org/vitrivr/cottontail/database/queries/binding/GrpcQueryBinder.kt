package org.vitrivr.cottontail.database.queries.binding

import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.extensions.*
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalNodeExpression
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
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.ProjectionPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.database.queries.predicates.bool.ConnectionOperator
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.math.knn.metrics.Distances
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
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
class GrpcQueryBinder constructor(val catalogue: Catalogue) {

    companion object {
        private val DEFAULT_PROJECTION = CottontailGrpc.Projection.newBuilder()
            .setOp(CottontailGrpc.Projection.ProjectionOperation.SELECT)
            .addColumns(
                CottontailGrpc.Projection.ProjectionElement.newBuilder()
                    .setColumn(CottontailGrpc.ColumnName.newBuilder().setName("*"))
            )
            .build()
    }

    /**
     * Binds the given [CottontailGrpc.Query] to the database objects and thereby creates a tree of [LogicalNodeExpression]s.
     *
     * @param query The [CottontailGrpc.Query] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @param transaction The [TransactionContext] used for binding.
     *
     * @return [LogicalNodeExpression]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(
        query: CottontailGrpc.Query,
        context: QueryContext,
        transaction: TransactionContext
    ): LogicalNodeExpression {
        /* Create FROM clause. */
        var root: LogicalNodeExpression = parseAndBindFrom(query.from, context, transaction)

        /* Create WHERE-clause. */
        root = if (query.hasWhere()) {
            parseAndBindBooleanPredicate(root, query.where, context)
        } else {
            root
        }

        /* Create kNN-clause . */
        root = if (query.hasKnn()) {
            parseAndBindKnnPredicate(root, query.knn, context)
        } else {
            root
        }

        /* Process SELECT-clause (projection). */
        root = if (query.hasProjection()) {
            parseAndBindProjection(root, query.projection, context)
        } else {
            parseAndBindProjection(root, DEFAULT_PROJECTION, context)
        }

        /* Process LIMIT and SKIP. */
        if (query.limit > 0L || query.skip > 0L) {
            val limit = LimitLogicalNodeExpression(query.limit, query.skip)
            limit.addInput(root)
            root = limit
        }
        context.logical = root
        return context.logical!!
    }

    /**
     * Binds the given [CottontailGrpc.InsertMessage] to the database objects and thereby creates
     * a tree of [LogicalNodeExpression]s.
     *
     * @param insert The [ CottontailGrpc.InsertMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @param transaction The [TransactionContext] used for binding.
     *
     * @return [LogicalNodeExpression]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(
        insert: CottontailGrpc.InsertMessage,
        context: QueryContext,
        transaction: TransactionContext
    ): LogicalNodeExpression {
        try {
            /* Parse entity for INSERT. */
            val entity = parseAndBindEntity(insert.from.scan.entity, context, transaction)
            val entityTx = transaction.getTx(entity) as EntityTx

            /* Parse columns to INSERT. */
            val values = insert.insertsList.map {
                val columnName = it.column.fqn()
                val column = entityTx.columnForName(columnName).columnDef
                val value = it.value.toValue(column)
                if (value == null) {
                    column to context.bindNull(column.type)
                } else {
                    column to context.bind(value)
                }
            }

            /* Create and return INSERT-clause. */
            val logical = context.logical
            if (logical is InsertLogicalNodeExpression) {
                logical.records.add(RecordBinding(logical.records.size.toLong(), values))
            } else {
                context.logical =
                    InsertLogicalNodeExpression(entity, mutableListOf(RecordBinding(0L, values)))
            }
            return context.logical!!
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
    }

    /**
     * Binds the given [CottontailGrpc.InsertMessage] and returns the [RecordBinding].
     *
     * @param insert The [ CottontailGrpc.InsertMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @param transaction The [TransactionContext] used for binding.
     *
     * @return [RecordBinding]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bindValues(
        insert: CottontailGrpc.InsertMessage,
        context: QueryContext,
        transaction: TransactionContext
    ): RecordBinding {
        try {
            /* Parse entity for INSERT. */
            val entity = parseAndBindEntity(insert.from.scan.entity, context, transaction)
            val entityTx = transaction.getTx(entity) as EntityTx

            /* Parse columns to INSERT. */
            val values = insert.insertsList.map {
                val columnName = it.column.fqn()
                val column = entityTx.columnForName(columnName).columnDef
                val value = it.value.toValue(column)
                if (value == null) {
                    column to context.bindNull(column.type)
                } else {
                    column to context.bind(value)
                }
            }

            return RecordBinding(0L, values)
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
    }

    /**
     * Binds the given [CottontailGrpc.UpdateMessage] to the database objects and thereby creates
     * a tree of [LogicalNodeExpression]s.
     *
     * @param update The [CottontailGrpc.UpdateMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @param transaction The [TransactionContext] used for binding.
     *
     * @return [LogicalNodeExpression]
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(
        update: CottontailGrpc.UpdateMessage,
        context: QueryContext,
        transaction: TransactionContext
    ) {
        try {
            /* Parse FROM-clause. */
            var root = parseAndBindFrom(update.from, context, transaction)
            if (root !is EntitySourceLogicalNodeExpression) {
                throw QueryException.QueryBindException("Failed to bind query. UPDATES only support entity sources as FROM-clause.")
            }
            val entity: Entity = root.entity

            /* Parse values to update. */
            val values = update.updatesList.map {
                val column = root.findUniqueColumnForName(it.column.fqn())
                val value = it.value.toValue(column)
                if (value == null) {
                    column to context.bindNull(column.type)
                } else {
                    column to context.bind(value)
                }
            }

            /* Create WHERE-clause. */
            root = if (update.hasWhere()) {
                parseAndBindBooleanPredicate(root, update.where, context)
            } else {
                root
            }

            /* Create and return UPDATE-clause. */
            val upd = UpdateLogicalNodeExpression(entity, values)
            upd.addInput(root)
            context.logical = upd
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
    }

    /**
     * Binds the given [CottontailGrpc.DeleteMessage] to the database objects and thereby creates
     * a tree of [LogicalNodeExpression]s.
     *
     * @param delete The [CottontailGrpc.DeleteMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @param transaction The [TransactionContext] used for binding.
     *
     * @return [LogicalNodeExpression]
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(
        delete: CottontailGrpc.DeleteMessage,
        context: QueryContext,
        transaction: TransactionContext
    ) {
        /* Parse FROM-clause. */
        val from = parseAndBindFrom(delete.from, context, transaction)
        if (from !is EntitySourceLogicalNodeExpression) {
            throw QueryException.QueryBindException("Failed to bind query. UPDATES only support entity sources as FROM-clause.")
        }
        val entity: Entity = from.entity
        var root: LogicalNodeExpression = from

        /* Create WHERE-clause. */
        root = if (delete.hasWhere()) {
            parseAndBindBooleanPredicate(root, delete.where, context)
        } else {
            root
        }

        /* Create and return DELETE-clause. */
        val del = DeleteLogicalNodeExpression(entity)
        del.addInput(root)
        context.logical = del
    }

    /**
     * Parses and binds a [CottontailGrpc.From] clause.
     *
     * @param from The [CottontailGrpc.From] object.
     * @param context The [QueryContext] used for binding.
     * @param transaction The [TransactionContext] used for binding.
     *
     * @return The resulting [LogicalNodeExpression].
     */
    private fun parseAndBindFrom(
        from: CottontailGrpc.From,
        context: QueryContext,
        transaction: TransactionContext
    ): LogicalNodeExpression = try {
        when (from.fromCase) {
            CottontailGrpc.From.FromCase.SCAN -> {
                val entity = parseAndBindEntity(from.scan.entity, context, transaction)
                val entityTx = transaction.getTx(entity) as EntityTx
                EntityScanLogicalNodeExpression(
                    entity = entity,
                    columns = entityTx.listColumns().map { it.columnDef }.toTypedArray()
                )
            }
            CottontailGrpc.From.FromCase.SAMPLE -> {
                val entity = parseAndBindEntity(from.scan.entity, context, transaction)
                val entityTx = transaction.getTx(entity) as EntityTx
                EntitySampleLogicalNodeExpression(
                    entity = entity,
                    columns = entityTx.listColumns().map { it.columnDef }.toTypedArray(),
                    size = from.sample.size,
                    seed = from.sample.seed
                )
            }
            CottontailGrpc.From.FromCase.SUBSELECT -> bind(
                from.subSelect,
                context,
                transaction
            ) /* Sub-select. */
            else -> throw QueryException.QuerySyntaxException("Invalid or missing FROM-clause in query.")
        }
    } catch (e: DatabaseException) {
        throw QueryException.QueryBindException("Failed to bind FROM due to database error: ${e.message}")
    }

    /**
     * Parses the given [CottontailGrpc.EntityName] and returns the corresponding [Entity].
     *
     * @param entity [CottontailGrpc.EntityName] to parse.
     * @param context The [QueryContext] used for query binding.
     * @param transaction The [TransactionContext] used for binding.

     * @return [Entity] that matches [CottontailGrpc.EntityName]
     */
    private fun parseAndBindEntity(
        entity: CottontailGrpc.EntityName,
        context: QueryContext,
        transaction: TransactionContext
    ): Entity = try {
        val name = entity.fqn()
        val catalogueTx = transaction.getTx(this.catalogue) as CatalogueTx
        val schemaTx = transaction.getTx(catalogueTx.schemaForName(name.schema())) as SchemaTx
        schemaTx.entityForName(name)
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${e.schema}'. Schema does not exist!")
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${e.entity}'. Entity does not exist!")
    }

    /**
     * Parses and binds a [CottontailGrpc.Where] clause.
     *
     * @param input The [LogicalNodeExpression] which to filter
     * @param where The [CottontailGrpc.Where] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [BooleanPredicate].
     */
    private fun parseAndBindBooleanPredicate(
        input: LogicalNodeExpression,
        where: CottontailGrpc.Where,
        context: QueryContext
    ): LogicalNodeExpression {
        val predicate = when (where.predicateCase) {
            CottontailGrpc.Where.PredicateCase.ATOMIC -> parseAndBindAtomicBooleanPredicate(
                input,
                where.atomic,
                context
            )
            CottontailGrpc.Where.PredicateCase.COMPOUND -> parseAndBindCompoundBooleanPredicate(
                input,
                where.compound,
                context
            )
            CottontailGrpc.Where.PredicateCase.PREDICATE_NOT_SET -> throw QueryException.QuerySyntaxException(
                "WHERE clause without a predicate is invalid!"
            )
            null -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
        }

        /* Generate FilterLogicalNodeExpression and return it. */
        val ret = FilterLogicalNodeExpression(predicate)
        ret.addInput(input)
        return ret
    }


    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param input The [LogicalNodeExpression] which to filter
     * @param compound The [CottontailGrpc.CompoundBooleanPredicate] object.
     * @param context The [QueryContext] used for query binding.

     * @return The resulting [BooleanPredicate.Compound].
     */
    private fun parseAndBindCompoundBooleanPredicate(
        input: LogicalNodeExpression,
        compound: CottontailGrpc.CompoundBooleanPredicate,
        context: QueryContext
    ): BooleanPredicateBinding.Compound {
        val left = when (compound.leftCase) {
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.ALEFT -> parseAndBindAtomicBooleanPredicate(
                input,
                compound.aleft,
                context
            )
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.CLEFT -> parseAndBindCompoundBooleanPredicate(
                input,
                compound.cleft,
                context
            )
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.LEFT_NOT_SET -> throw QueryException.QuerySyntaxException(
                "Unbalanced predicate! A compound boolean predicate must have a left and a right side."
            )
            null -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
        }

        val right = when (compound.rightCase) {
            CottontailGrpc.CompoundBooleanPredicate.RightCase.ARIGHT -> parseAndBindAtomicBooleanPredicate(
                input,
                compound.aright,
                context
            )
            CottontailGrpc.CompoundBooleanPredicate.RightCase.CRIGHT -> parseAndBindCompoundBooleanPredicate(
                input,
                compound.cright,
                context
            )
            CottontailGrpc.CompoundBooleanPredicate.RightCase.RIGHT_NOT_SET -> throw QueryException.QuerySyntaxException(
                "Unbalanced predicate! A compound boolean predicate must have a left and a right side."
            )
            null -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
        }

        return try {
            BooleanPredicateBinding.Compound(
                ConnectionOperator.valueOf(compound.op.name),
                left,
                right
            )
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("'${compound.op.name}' is not a valid connection operator for a boolean predicate!")
        }
    }

    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param input The [LogicalNodeExpression] which to filter
     * @param atomic The [CottontailGrpc.AtomicLiteralBooleanPredicate] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [BooleanPredicate.Atomic].
     */
    private fun parseAndBindAtomicBooleanPredicate(
        input: LogicalNodeExpression,
        atomic: CottontailGrpc.AtomicLiteralBooleanPredicate,
        context: QueryContext
    ): BooleanPredicateBinding.Atomic {
        /* Parse and bind column name to input */
        val columnName = atomic.left.fqn()
        val column = input.findUniqueColumnForName(columnName)

        /* Parse and bind operator. */
        val operator = try {
            ComparisonOperator.valueOf(atomic.op.name)
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("'${atomic.op.name}' is not a valid comparison operator for a boolean predicate!")
        }

        /* Return the resulting AtomicBooleanPredicate. */
        return BooleanPredicateBinding.Atomic(column, operator, atomic.not, atomic.rightList.map {
            val v = it.toValue(column)
                ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.")
            when (operator) {
                ComparisonOperator.LIKE -> {
                    if (v is StringValue) {
                        context.bind(LikePatternValue(v.value))
                    } else {
                        throw QueryException.QuerySyntaxException("LIKE operator requires a parsable string value as second operand.")
                    }
                }
                ComparisonOperator.MATCH -> {
                    if (v is StringValue) {
                        context.bind(LucenePatternValue(v.value))
                    } else {
                        throw QueryException.QuerySyntaxException("MATCH operator requires a parsable string value as second operand.")
                    }
                }
                else -> context.bind(v)
            }
        })
    }

    /**
     * Parses and binds the kNN-lookup part of a GRPC [CottontailGrpc.Query]
     *
     * @param input The [LogicalNodeExpression] which to perform the kNN
     * @param knn The [CottontailGrpc.Knn] object.
     * @param context The [QueryContext] used for query binding.

     * @return The resulting [KnnPredicate].
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseAndBindKnnPredicate(
        input: LogicalNodeExpression,
        knn: CottontailGrpc.Knn,
        context: QueryContext
    ): LogicalNodeExpression {
        val columnName = knn.attribute.fqn()
        val column = input.findUniqueColumnForName(columnName)
        val distance = Distances.valueOf(knn.distance.name).kernel
        val hint = knn.hint.toHint()

        val predicate = when (column.type) {
            is Type.DoubleVector -> {
                val query = knn.queryList.map { q -> context.bind(q.toDoubleVectorValue()) }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { w -> context.bind(w.toDoubleVectorValue()) }
                    KnnPredicateBinding(
                        column = column as ColumnDef<DoubleVectorValue>,
                        k = knn.k,
                        query = query,
                        weights = weights,
                        distance = distance,
                        hint = hint
                    )
                } else {
                    KnnPredicateBinding(
                        column = column as ColumnDef<DoubleVectorValue>,
                        k = knn.k,
                        query = query,
                        distance = distance,
                        hint = hint
                    )
                }
            }
            is Type.FloatVector -> {
                val query = knn.queryList.map { q -> context.bind(q.toFloatVectorValue()) }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { context.bind(it.toFloatVectorValue()) }
                    KnnPredicateBinding(
                        column = column as ColumnDef<FloatVectorValue>,
                        k = knn.k,
                        query = query,
                        weights = weights,
                        distance = distance,
                        hint = hint
                    )
                } else {
                    KnnPredicateBinding(
                        column = column as ColumnDef<FloatVectorValue>,
                        k = knn.k,
                        query = query,
                        distance = distance,
                        hint = hint
                    )
                }
            }
            is Type.LongVector -> {
                val query = knn.queryList.map { q -> context.bind(q.toLongVectorValue()) }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { context.bind(it.toLongVectorValue()) }
                    KnnPredicateBinding(
                        column = column as ColumnDef<LongVectorValue>,
                        k = knn.k,
                        query = query,
                        weights = weights,
                        distance = distance,
                        hint = hint
                    )
                } else {
                    KnnPredicateBinding(
                        column = column as ColumnDef<LongVectorValue>,
                        k = knn.k,
                        query = query,
                        distance = distance,
                        hint = hint
                    )
                }
            }
            is Type.IntVector -> {
                val query = knn.queryList.map { q -> context.bind(q.toIntVectorValue()) }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { context.bind(it.toIntVectorValue()) }
                    KnnPredicateBinding(
                        column = column as ColumnDef<IntVectorValue>,
                        k = knn.k,
                        query = query,
                        weights = weights,
                        distance = distance,
                        hint = hint
                    )
                } else {
                    KnnPredicateBinding(
                        column = column as ColumnDef<IntVectorValue>,
                        k = knn.k,
                        query = query,
                        distance = distance,
                        hint = hint
                    )
                }
            }
            is Type.BooleanVector -> {
                val query = knn.queryList.map { q -> context.bind(q.toBooleanVectorValue()) }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { context.bind(it.toBooleanVectorValue()) }
                    KnnPredicateBinding(
                        column = column as ColumnDef<BooleanVectorValue>,
                        k = knn.k,
                        query = query,
                        weights = weights,
                        distance = distance,
                        hint = hint
                    )
                } else {
                    KnnPredicateBinding(
                        column = column as ColumnDef<BooleanVectorValue>,
                        k = knn.k,
                        query = query,
                        distance = distance,
                        hint = hint
                    )
                }
            }
            is Type.Complex32Vector -> {
                val query = knn.queryList.map { q -> context.bind(q.toComplex32VectorValue()) }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { context.bind(it.toComplex32VectorValue()) }
                    KnnPredicateBinding(
                        column = column as ColumnDef<Complex32VectorValue>,
                        k = knn.k,
                        query = query,
                        weights = weights,
                        distance = distance,
                        hint = hint
                    )
                } else {
                    KnnPredicateBinding(
                        column = column as ColumnDef<Complex32VectorValue>,
                        k = knn.k,
                        query = query,
                        distance = distance,
                        hint = hint
                    )
                }
            }
            is Type.Complex64Vector -> {
                val query = knn.queryList.map { q -> context.bind(q.toComplex64VectorValue()) }
                if (knn.weightsCount > 0) {
                    val weights = knn.weightsList.map { context.bind(it.toComplex64VectorValue()) }
                    KnnPredicateBinding(
                        column = column as ColumnDef<Complex64VectorValue>,
                        k = knn.k,
                        query = query,
                        weights = weights,
                        distance = distance,
                        hint = hint
                    )
                } else {
                    KnnPredicateBinding(
                        column = column as ColumnDef<Complex64VectorValue>,
                        k = knn.k,
                        query = query,
                        distance = distance,
                        hint = hint
                    )
                }
            }
            else -> throw QueryException.QuerySyntaxException("A kNN predicate does not contain a valid query vector!")
        }

        /* Generate KnnLogicalNodeExpression and return it. */
        val ret = KnnLogicalNodeExpression(predicate)
        ret.addInput(input)
        return ret
    }

    /**
     * Parses and binds the projection part of a gRPC [CottontailGrpc.Query]
     *
     * @param input The [LogicalNodeExpression] on which to perform projection.
     * @param projection The [CottontailGrpc.Projection] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [ProjectionPhysicalNodeExpression].
     */
    private fun parseAndBindProjection(
        input: LogicalNodeExpression,
        projection: CottontailGrpc.Projection,
        context: QueryContext
    ): LogicalNodeExpression {
        val fields = projection.columnsList.flatMap { p ->
            input.findColumnsForName(p.column.fqn()).map {
                if (p.hasAlias()) {
                    it to p.alias.fqn()
                } else {
                    it to null
                }
            }
        }
        val type = try {
            Projection.valueOf(projection.op.name)
        } catch (e: java.lang.IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): ${projection.op} is not supported.")
        }

        /* Generate KnnLogicalNodeExpression and return it. */
        val ret = ProjectionLogicalNodeExpression(type, fields)
        ret.addInput(input)
        return ret
    }

    /**
     * Tries to find and return a [ColumnDef] that matches the given [Name.ColumnName] in
     * this [LogicalNodeExpression]. The match must be unique!
     *
     * @param name [Name.ColumnName] to look for.
     * @return [ColumnDef] that uniquely matches the [Name.ColumnName]
     */
    private fun LogicalNodeExpression.findUniqueColumnForName(name: Name.ColumnName): ColumnDef<*> {
        val candidates = this.findColumnsForName(name)
        if (candidates.isEmpty()) throw QueryException.QueryBindException("Could not find column '$name' in input.")
        if (candidates.size > 1) throw QueryException.QueryBindException("Multiple candidates for column '$name' in input.")
        return candidates.first()
    }

    /**
     * Tries to find and return all [ColumnDef]s that matches the given [Name.ColumnName] in this [LogicalNodeExpression].
     *
     * @param name [Name.ColumnName] to look for.
     * @return List of [ColumnDef] that  match the [Name.ColumnName]
     */
    private fun LogicalNodeExpression.findColumnsForName(name: Name.ColumnName): List<ColumnDef<*>> =
        this.columns.filter { name.matches(it.name) }
}