package org.vitrivr.cottontail.database.queries.binding

import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.extensions.*
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.LogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.DeleteLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.InsertLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.UpdateLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.KnnLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.LimitLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.ProjectionLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntitySampleLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntitySourceLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.ProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.database.queries.predicates.bool.ConnectionOperator
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.math.knn.metrics.Distances
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.pattern.LikePatternValue
import org.vitrivr.cottontail.model.values.pattern.LucenePatternValue

/**
 * This helper class parses and binds queries issued through the gRPC endpoint. The process encompasses three steps:
 *
 * 1) The [CottontailGrpc.Query] is decomposed into its components.
 * 2) The gRPC query components are bound to Cottontail DB objects and internal query objects are constructed. This step includes some basic validation.
 * 3) A [LogicalOperatorNode] tree is constructed from the internal query objects.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class GrpcQueryBinder constructor(val catalogue: Catalogue) {

    companion object {
        private val DEFAULT_PROJECTION = CottontailGrpc.Projection.newBuilder()
            .setOp(CottontailGrpc.Projection.ProjectionOperation.SELECT)
            .addColumns(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("*")))
            .build()
    }

    /**
     * Binds the given [CottontailGrpc.Query] to the database objects and thereby creates a tree of [LogicalOperatorNode]s.
     *
     * @param query The [CottontailGrpc.Query] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @param transaction The [TransactionContext] used for binding.
     *
     * @return [LogicalOperatorNode]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(
        query: CottontailGrpc.Query,
        context: QueryContext,
        transaction: TransactionContext
    ): LogicalOperatorNode {
        /* Create FROM clause. */
        var root: LogicalOperatorNode = parseAndBindFrom(query.from, context, transaction)

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
            val limit = LimitLogicalOperatorNode(query.limit, query.skip)
            limit.addInput(root)
            root = limit
        }
        context.logical = root
        return context.logical!!
    }

    /**
     * Binds the given [CottontailGrpc.InsertMessage] to the database objects and thereby creates
     * a tree of [LogicalOperatorNode]s.
     *
     * @param insert The [ CottontailGrpc.InsertMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @param transaction The [TransactionContext] used for binding.
     *
     * @return [LogicalOperatorNode]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(
        insert: CottontailGrpc.InsertMessage,
        context: QueryContext,
        transaction: TransactionContext
    ): LogicalOperatorNode {
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
            if (logical is InsertLogicalOperatorNode) {
                logical.records.add(RecordBinding(logical.records.size.toLong(), values))
            } else {
                context.logical =
                    InsertLogicalOperatorNode(entity, mutableListOf(RecordBinding(0L, values)))
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
    fun bindValues(insert: CottontailGrpc.InsertMessage, context: QueryContext, transaction: TransactionContext) : RecordBinding {
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
     * a tree of [LogicalOperatorNode]s.
     *
     * @param update The [CottontailGrpc.UpdateMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @param transaction The [TransactionContext] used for binding.
     *
     * @return [LogicalOperatorNode]
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(update: CottontailGrpc.UpdateMessage, context: QueryContext, transaction: TransactionContext) {
        try {
            /* Parse FROM-clause. */
            var root = parseAndBindFrom(update.from, context, transaction)
            if (root !is EntitySourceLogicalOperatorNode) {
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
            val upd = UpdateLogicalOperatorNode(entity, values)
            upd.addInput(root)
            context.logical = upd
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
    }

    /**
     * Binds the given [CottontailGrpc.DeleteMessage] to the database objects and thereby creates
     * a tree of [LogicalOperatorNode]s.
     *
     * @param delete The [CottontailGrpc.DeleteMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @param transaction The [TransactionContext] used for binding.
     *
     * @return [LogicalOperatorNode]
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(delete: CottontailGrpc.DeleteMessage, context: QueryContext, transaction: TransactionContext) {
        /* Parse FROM-clause. */
        val from = parseAndBindFrom(delete.from, context, transaction)
        if (from !is EntitySourceLogicalOperatorNode) {
            throw QueryException.QueryBindException("Failed to bind query. UPDATES only support entity sources as FROM-clause.")
        }
        val entity: Entity = from.entity
        var root: LogicalOperatorNode = from

        /* Create WHERE-clause. */
        root = if (delete.hasWhere()) {
            parseAndBindBooleanPredicate(root, delete.where, context)
        } else {
            root
        }

        /* Create and return DELETE-clause. */
        val del = DeleteLogicalOperatorNode(entity)
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
     * @return The resulting [LogicalOperatorNode].
     */
    private fun parseAndBindFrom(
        from: CottontailGrpc.From,
        context: QueryContext,
        transaction: TransactionContext
    ): LogicalOperatorNode = try {
        when (from.fromCase) {
            CottontailGrpc.From.FromCase.SCAN -> {
                val entity = parseAndBindEntity(from.scan.entity, context, transaction)
                val entityTx = transaction.getTx(entity) as EntityTx
                EntityScanLogicalOperatorNode(
                    entity = entity,
                    columns = entityTx.listColumns().map { it.columnDef }.toTypedArray()
                )
            }
            CottontailGrpc.From.FromCase.SAMPLE -> {
                val entity = parseAndBindEntity(from.scan.entity, context, transaction)
                val entityTx = transaction.getTx(entity) as EntityTx
                EntitySampleLogicalOperatorNode(
                    entity = entity,
                    columns = entityTx.listColumns().map { it.columnDef }.toTypedArray(),
                    size = from.sample.size,
                    seed = from.sample.seed
                )
            }
            CottontailGrpc.From.FromCase.SUBSELECT -> bind(from.subSelect, context, transaction) /* Sub-select. */
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
    private fun parseAndBindEntity(entity: CottontailGrpc.EntityName, context: QueryContext, transaction: TransactionContext): Entity = try {
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
     * @param input The [LogicalOperatorNode] which to filter
     * @param where The [CottontailGrpc.Where] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [BooleanPredicate].
     */
    private fun parseAndBindBooleanPredicate(
        input: LogicalOperatorNode,
        where: CottontailGrpc.Where,
        context: QueryContext
    ): LogicalOperatorNode {
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
        val ret = FilterLogicalOperatorNode(predicate)
        ret.addInput(input)
        return ret
    }


    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param input The [LogicalOperatorNode] which to filter
     * @param compound The [CottontailGrpc.CompoundBooleanPredicate] object.
     * @param context The [QueryContext] used for query binding.

     * @return The resulting [BooleanPredicate.Compound].
     */
    private fun parseAndBindCompoundBooleanPredicate(
        input: LogicalOperatorNode,
        compound: CottontailGrpc.CompoundBooleanPredicate,
        context: QueryContext
    ): BooleanPredicate.Compound {
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
            CottontailGrpc.CompoundBooleanPredicate.RightCase.RIGHT_NOT_SET -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
            null -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
        }

        return try {
            BooleanPredicate.Compound(ConnectionOperator.valueOf(compound.op.name), left, right)
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("'${compound.op.name}' is not a valid connection operator for a boolean predicate!")
        }
    }

    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param input The [LogicalOperatorNode] which to filter
     * @param atomic The [CottontailGrpc.AtomicLiteralBooleanPredicate] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [BooleanPredicate.Atomic].
     */
    private fun parseAndBindAtomicBooleanPredicate(
        input: LogicalOperatorNode,
        atomic: CottontailGrpc.AtomicLiteralBooleanPredicate,
        context: QueryContext
    ): BooleanPredicate.Atomic {
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
        val ret = BooleanPredicate.Atomic(column, operator, atomic.not)
        atomic.rightList.forEach {
            val v = it.toValue(column)
                ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.")
            val binding = when (operator) {
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
            ret.value(binding)
        }
        return ret
    }

    /**
     * Parses and binds the kNN-lookup part of a GRPC [CottontailGrpc.Query]
     *
     * @param input The [LogicalOperatorNode] which to perform the kNN
     * @param knn The [CottontailGrpc.Knn] object.
     * @param context The [QueryContext] used for query binding.

     * @return The resulting [KnnPredicate].
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseAndBindKnnPredicate(
        input: LogicalOperatorNode,
        knn: CottontailGrpc.Knn,
        context: QueryContext
    ): LogicalOperatorNode {
        val columnName = knn.attribute.fqn()
        val column = input.findUniqueColumnForName(columnName)
        val distance = Distances.valueOf(knn.distance.name).kernel
        val hint = knn.hint.toHint()

        val predicate = KnnPredicate(column = column, k = knn.k, distance = distance, hint = hint)
        when (column.type) {
            is Type.DoubleVector -> {
                knn.queryList.forEach { q -> predicate.query(context.bind(q.toDoubleVectorValue())) }
                knn.weightsList.forEach { w -> predicate.weight(w.toDoubleVectorValue()) }
            }
            is Type.FloatVector -> {
                knn.queryList.forEach { q -> predicate.query(context.bind(q.toFloatVectorValue())) }
                knn.weightsList.forEach { w -> predicate.weight(w.toFloatVectorValue()) }
            }
            is Type.LongVector -> {
                knn.queryList.forEach { q -> predicate.query(context.bind(q.toLongVectorValue())) }
                knn.weightsList.forEach { w -> predicate.weight(w.toLongVectorValue()) }
            }
            is Type.IntVector -> {
                knn.queryList.forEach { q -> predicate.query(context.bind(q.toIntVectorValue())) }
                knn.weightsList.forEach { w -> predicate.weight(w.toIntVectorValue()) }
            }
            is Type.BooleanVector -> {
                knn.queryList.forEach { q -> predicate.query(context.bind(q.toBooleanVectorValue())) }
                knn.weightsList.forEach { w -> predicate.weight(w.toBooleanVectorValue()) }
            }
            is Type.Complex32Vector -> {
                knn.queryList.forEach { q -> predicate.query(context.bind(q.toComplex32VectorValue())) }
                knn.weightsList.forEach { w -> predicate.weight(w.toComplex32VectorValue()) }
            }
            is Type.Complex64Vector -> {
                knn.queryList.forEach { q -> predicate.query(context.bind(q.toComplex64VectorValue())) }
                knn.weightsList.forEach { w -> predicate.weight(w.toComplex64VectorValue()) }
            }
            else -> throw QueryException.QuerySyntaxException("A kNN predicate does not contain a valid query vector!")
        }

        /* Generate KnnLogicalNodeExpression and return it. */
        val ret = KnnLogicalOperatorNode(predicate)
        ret.addInput(input)
        return ret
    }

    /**
     * Parses and binds the projection part of a gRPC [CottontailGrpc.Query]
     *
     * @param input The [LogicalOperatorNode] on which to perform projection.
     * @param projection The [CottontailGrpc.Projection] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [ProjectionPhysicalOperatorNode].
     */
    private fun parseAndBindProjection(
        input: LogicalOperatorNode,
        projection: CottontailGrpc.Projection,
        context: QueryContext
    ): LogicalOperatorNode {
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
        val ret = ProjectionLogicalOperatorNode(type, fields)
        ret.addInput(input)
        return ret
    }

    /**
     * Tries to find and return a [ColumnDef] that matches the given [Name.ColumnName] in
     * this [LogicalOperatorNode]. The match must be unique!
     *
     * @param name [Name.ColumnName] to look for.
     * @return [ColumnDef] that uniquely matches the [Name.ColumnName]
     */
    private fun LogicalOperatorNode.findUniqueColumnForName(name: Name.ColumnName): ColumnDef<*> {
        val candidates = this.findColumnsForName(name)
        if (candidates.isEmpty()) throw QueryException.QueryBindException("Could not find column '$name' in input.")
        if (candidates.size > 1) throw QueryException.QueryBindException("Multiple candidates for column '$name' in input.")
        return candidates.first()
    }

    /**
     * Tries to find and return all [ColumnDef]s that matches the given [Name.ColumnName] in this [LogicalOperatorNode].
     *
     * @param name [Name.ColumnName] to look for.
     * @return List of [ColumnDef] that  match the [Name.ColumnName]
     */
    private fun LogicalOperatorNode.findColumnsForName(name: Name.ColumnName): List<ColumnDef<*>> =
        this.columns.filter { name.matches(it.name) }
}