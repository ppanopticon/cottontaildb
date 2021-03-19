package org.vitrivr.cottontail.database.queries.binding

import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.extensions.*
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.DeleteLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.InsertLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.management.UpdateLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterOnSubSelectLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.*
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sort.SortLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntitySampleLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform.DistanceLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform.LimitLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.database.queries.predicates.bool.ConnectionOperator
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.math.knn.metrics.Distances
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.pattern.LikePatternValue
import org.vitrivr.cottontail.model.values.pattern.LucenePatternValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.math.KnnUtilities

/**
 * This helper class parses and binds queries issued through the gRPC endpoint. The process encompasses three steps:
 *
 * 1) The [CottontailGrpc.Query] is decomposed into its components.
 * 2) The gRPC query components are bound to Cottontail DB objects and internal query objects are constructed. This step includes some basic validation.
 * 3) A [OperatorNode.Logical] tree is constructed from the internal query objects.
 *
 * @author Ralph Gasser
 * @version 1.6.1
 */
class GrpcQueryBinder constructor(val catalogue: Catalogue) {

    companion object {
        private val DEFAULT_PROJECTION = CottontailGrpc.Projection.newBuilder()
            .setOp(CottontailGrpc.Projection.ProjectionOperation.SELECT)
            .addColumns(CottontailGrpc.Projection.ProjectionElement.newBuilder().setColumn(CottontailGrpc.ColumnName.newBuilder().setName("*")))
            .build()
    }

    /**
     * Binds the given [CottontailGrpc.Query] to the database objects and thereby creates a tree of [OperatorNode.Logical]s.
     *
     * @param query The [CottontailGrpc.Query] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @return [OperatorNode.Logical]
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(query: CottontailGrpc.Query, context: QueryContext): OperatorNode.Logical {
        /* Create FROM clause. */
        var root: OperatorNode.Logical = parseAndBindFrom(query.from, context)

        /* Create WHERE-clause. */
        root = if (query.hasWhere()) {
            parseAndBindBooleanPredicate(root, query.where, context)
        } else {
            root
        }

        /* Parse and bind kNN-clause . */
        root = if (query.hasKnn()) {
            parseAndBindKnn(root, query.knn, context)
        } else {
            root
        }

        /* Parse and bind ORDER-clause . */
        root = if (query.hasOrder()) {
            parseAndBindOrder(root, query.order, context)
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
        root = if (query.limit > 0L || query.skip > 0L) {
            LimitLogicalOperatorNode(root, query.limit, query.skip)
        } else {
            root
        }
        context.register(root)
        return root
    }

    /**
     * Binds the given [CottontailGrpc.InsertMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param insert The [ CottontailGrpc.InsertMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(insert: CottontailGrpc.InsertMessage, context: QueryContext) {
        try {
            /* Parse entity for INSERT. */
            val entity = parseAndBindEntity(insert.from.scan.entity, context)
            val entityTx = context.txn.getTx(entity) as EntityTx

            /* Parse columns to INSERT. */
            val values = insert.insertsList.map {
                val columnName = it.column.fqn()
                val column = entityTx.columnForName(columnName).columnDef
                column to it.value.toValue(column)
            }

            /* Create and return INSERT-clause. */
            val record = context.records.bind(StandaloneRecord(-1L, values))
            context.register(InsertLogicalOperatorNode(context.nextGroupId(), entity, mutableListOf(record)))
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
    }

    /**
     * Binds the given [CottontailGrpc.InsertMessage] and returns the [RecordBinding].
     *
     * @param insert The [ CottontailGrpc.InsertMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     *
     * @return [RecordBinding]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bindValues(insert: CottontailGrpc.InsertMessage, context: QueryContext): Binding<Record> {
        try {
            /* Parse entity for INSERT. */
            val entity = parseAndBindEntity(insert.from.scan.entity, context)
            val entityTx = context.txn.getTx(entity) as EntityTx

            /* Parse columns to INSERT. */
            val values = insert.insertsList.map {
                val columnName = it.column.fqn()
                val column = entityTx.columnForName(columnName).columnDef
                column to it.value.toValue(column)
            }
            return context.records.bind(StandaloneRecord(-1L, values))
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
    }

    /**
     * Binds the given [CottontailGrpc.UpdateMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param update The [CottontailGrpc.UpdateMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(update: CottontailGrpc.UpdateMessage, context: QueryContext) {
        try {
            /* Parse FROM-clause. */
            var root = parseAndBindFrom(update.from, context)
            if (root !is EntityScanLogicalOperatorNode) {
                throw QueryException.QueryBindException("Failed to bind query. UPDATES only support entity sources as FROM-clause.")
            }
            val entity: Entity = root.entity

            /* Parse values to update. */
            val values = update.updatesList.map {
                val column = root.findUniqueColumnForName(it.column.fqn())
                val value = it.value.toValue(column)
                column to context.values.bind(value)
            }

            /* Create WHERE-clause. */
            root = if (update.hasWhere()) {
                parseAndBindBooleanPredicate(root, update.where, context)
            } else {
                root
            }

            /* Create and return UPDATE-clause. */
            context.register(UpdateLogicalOperatorNode(root, entity, values))
        } catch (e: DatabaseException.ColumnDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${e.column}'. Column does not exist!")
        }
    }

    /**
     * Binds the given [CottontailGrpc.DeleteMessage] to the database objects and thereby creates
     * a tree of [OperatorNode.Logical]s.
     *
     * @param delete The [CottontailGrpc.DeleteMessage] that should be bound.
     * @param context The [QueryContext] used for binding.
     *
     * @return [OperatorNode.Logical]
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun bind(delete: CottontailGrpc.DeleteMessage, context: QueryContext) {
        /* Parse FROM-clause. */
        val from = parseAndBindFrom(delete.from, context)
        if (from !is EntityScanLogicalOperatorNode) {
            throw QueryException.QueryBindException("Failed to bind query. UPDATES only support entity sources as FROM-clause.")
        }
        val entity: Entity = from.entity
        var root: OperatorNode.Logical = from

        /* Create WHERE-clause. */
        root = if (delete.hasWhere()) {
            parseAndBindBooleanPredicate(root, delete.where, context)
        } else {
            root
        }

        /* Create and return DELETE-clause. */
        context.register(DeleteLogicalOperatorNode(root, entity))
    }

    /**
     * Parses and binds a [CottontailGrpc.From] clause.
     *
     * @param from The [CottontailGrpc.From] object.
     * @param context The [QueryContext] used for binding.
     *
     * @return The resulting [OperatorNode.Logical].
     */
    private fun parseAndBindFrom(from: CottontailGrpc.From, context: QueryContext): OperatorNode.Logical = try {
        when (from.fromCase) {
            CottontailGrpc.From.FromCase.SCAN -> {
                val entity = parseAndBindEntity(from.scan.entity, context)
                val entityTx = context.txn.getTx(entity) as EntityTx
                val columns = entityTx.listColumns().map { it.columnDef }.toTypedArray()
                EntityScanLogicalOperatorNode(context.nextGroupId(), entity = entity, columns = columns)
            }
            CottontailGrpc.From.FromCase.SAMPLE -> {
                val entity = parseAndBindEntity(from.scan.entity, context)
                val entityTx = context.txn.getTx(entity) as EntityTx
                val columns = entityTx.listColumns().map { it.columnDef }.toTypedArray()
                EntitySampleLogicalOperatorNode(context.nextGroupId(), entity = entity, columns = columns, size = from.sample.size, seed = from.sample.seed)
            }
            CottontailGrpc.From.FromCase.SUBSELECT -> bind(from.subSelect, context) /* Sub-select. */
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

     * @return [Entity] that matches [CottontailGrpc.EntityName]
     */
    private fun parseAndBindEntity(entity: CottontailGrpc.EntityName, context: QueryContext): Entity = try {
        val name = entity.fqn()
        val catalogueTx = context.txn.getTx(this.catalogue) as CatalogueTx
        val schemaTx = context.txn.getTx(catalogueTx.schemaForName(name.schema())) as SchemaTx
        schemaTx.entityForName(name)
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${e.schema}'. Schema does not exist!")
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        throw QueryException.QueryBindException("Failed to bind '${e.entity}'. Entity does not exist!")
    }

    /**
     * Parses and binds a [CottontailGrpc.Where] clause.
     *
     * @param input The [OperatorNode.Logical] which to filter
     * @param where The [CottontailGrpc.Where] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [BooleanPredicate].
     */
    private fun parseAndBindBooleanPredicate(input: OperatorNode.Logical, where: CottontailGrpc.Where, context: QueryContext): OperatorNode.Logical {
        val predicate = when (where.predicateCase) {
            CottontailGrpc.Where.PredicateCase.ATOMIC -> parseAndBindAtomicBooleanPredicate(input, where.atomic, context)
            CottontailGrpc.Where.PredicateCase.COMPOUND -> parseAndBindCompoundBooleanPredicate(input, where.compound, context)
            CottontailGrpc.Where.PredicateCase.PREDICATE_NOT_SET -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
            null -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
        }

        /* Generate FilterLogicalNodeExpression and return it. */
        val subQuery = predicate.atomics.filterIsInstance<BooleanPredicate.Atomic.Literal>().filter { it.dependsOn > 0 }.map { context[it.dependsOn] }
        if (subQuery.isNotEmpty()) {
            return FilterOnSubSelectLogicalOperatorNode(predicate, input, *subQuery.toTypedArray())
        } else {
            return FilterLogicalOperatorNode(input, predicate)
        }
    }


    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param input The [OperatorNode.Logical] which to filter
     * @param compound The [CottontailGrpc.CompoundBooleanPredicate] object.
     * @param context The [QueryContext] used for query binding.

     * @return The resulting [BooleanPredicate.Compound].
     */
    private fun parseAndBindCompoundBooleanPredicate(input: OperatorNode.Logical, compound: CottontailGrpc.CompoundBooleanPredicate, context: QueryContext): BooleanPredicate.Compound {
        val left = when (compound.leftCase) {
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.ALEFT -> parseAndBindAtomicBooleanPredicate(input, compound.aleft, context)
            CottontailGrpc.CompoundBooleanPredicate.LeftCase.CLEFT -> parseAndBindCompoundBooleanPredicate(input, compound.cleft, context)
            else -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
        }

        val right = when (compound.rightCase) {
            CottontailGrpc.CompoundBooleanPredicate.RightCase.ARIGHT -> parseAndBindAtomicBooleanPredicate(input, compound.aright, context)
            CottontailGrpc.CompoundBooleanPredicate.RightCase.CRIGHT -> parseAndBindCompoundBooleanPredicate(input, compound.cright, context)
            else -> throw QueryException.QuerySyntaxException("Unbalanced predicate! A compound boolean predicate must have a left and a right side.")
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
     * @param input The [OperatorNode.Logical] which to filter
     * @param atomic The [CottontailGrpc.AtomicBooleanPredicate] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [BooleanPredicate.Atomic].
     */
    private fun parseAndBindAtomicBooleanPredicate(input: OperatorNode.Logical, atomic: CottontailGrpc.AtomicBooleanPredicate, context: QueryContext): BooleanPredicate.Atomic {
        /* Parse and bind column name to input */
        val left = input.findUniqueColumnForName(atomic.left.fqn())
        return when (atomic.right.operandCase) {
            CottontailGrpc.AtomicBooleanOperand.OperandCase.COLUMN -> {
                val operator = parseUnboundOperator(atomic.op, context)
                val right = input.findUniqueColumnForName(atomic.right.column.fqn())
                if (operator is ComparisonOperator.Binary) {
                    BooleanPredicate.Atomic.Reference(left, right, operator, atomic.not)
                } else {
                    throw QueryException.QuerySyntaxException("Reference based comparison with another column requires a binary operator (i.e., ==, >, <, >=, <= or LIKE).")
                }
            }
            CottontailGrpc.AtomicBooleanOperand.OperandCase.LITERALS -> {
                val operand = parseAndBindOperator(atomic.op, atomic.right.literals.literalList.mapNotNull { it.toValue(left) }, context)
                BooleanPredicate.Atomic.Literal(left, operand, atomic.not)
            }
            CottontailGrpc.AtomicBooleanOperand.OperandCase.QUERY -> {
                val subQuery = this.bind(atomic.right.query, context)
                val operand = parseAndBindOperator(atomic.op, atomic.right.literals.literalList.mapNotNull { it.toValue(left) }, context)
                BooleanPredicate.Atomic.Literal(left, operand, atomic.not, subQuery.groupId)
            }
            else -> throw QueryException.QuerySyntaxException("Failed to parse operand for atomic boolean predicate.")
        }
    }

    /**
     * Simply parses a [CottontailGrpc.ComparisonOperator]
     *
     * @param operator: CottontailGrpc.ComparisonOperator To parse.
     * @return [ComparisonOperator]
     */
    private fun parseUnboundOperator(operator: CottontailGrpc.ComparisonOperator, context: QueryContext) = when (operator) {
        CottontailGrpc.ComparisonOperator.ISNULL -> ComparisonOperator.IsNull()
        CottontailGrpc.ComparisonOperator.EQUAL -> ComparisonOperator.Binary.Equal(Binding(0))
        CottontailGrpc.ComparisonOperator.GREATER -> ComparisonOperator.Binary.Greater(Binding(0))
        CottontailGrpc.ComparisonOperator.LESS -> ComparisonOperator.Binary.LessEqual(Binding(0))
        CottontailGrpc.ComparisonOperator.GEQUAL -> ComparisonOperator.Binary.GreaterEqual(Binding(0))
        CottontailGrpc.ComparisonOperator.LEQUAL -> ComparisonOperator.Binary.LessEqual(Binding(0))
        CottontailGrpc.ComparisonOperator.LIKE -> ComparisonOperator.Binary.Like(Binding(0))
        CottontailGrpc.ComparisonOperator.MATCH -> ComparisonOperator.Binary.Match(Binding(0))
        CottontailGrpc.ComparisonOperator.BETWEEN -> ComparisonOperator.Between(Binding(0), Binding(1))
        CottontailGrpc.ComparisonOperator.IN -> ComparisonOperator.In(mutableListOf())
        CottontailGrpc.ComparisonOperator.UNRECOGNIZED -> throw QueryException.QuerySyntaxException("Operator ${operator.name} is not a valid comparison operator for a boolean predicate!")
    }

    /**
     * Parses a [CottontailGrpc.ComparisonOperator] and binds literal [Value]s to it.
     *
     * @param operator: CottontailGrpc.ComparisonOperator To parse.
     * @param literals List of [Value]s to register with to the resulting [ComparisonOperator].
     * @param context The [QueryContext] used for query binding.
     * @return [ComparisonOperator]
     */
    private fun parseAndBindOperator(operator: CottontailGrpc.ComparisonOperator, literals: List<Value>, context: QueryContext): ComparisonOperator {
        /* Prepare operator. */
        return when (operator) {
            CottontailGrpc.ComparisonOperator.ISNULL -> ComparisonOperator.IsNull()
            CottontailGrpc.ComparisonOperator.EQUAL -> ComparisonOperator.Binary.Equal(context.values.bind(literals.first()))
            CottontailGrpc.ComparisonOperator.GREATER -> ComparisonOperator.Binary.Greater(context.values.bind(literals.first()))
            CottontailGrpc.ComparisonOperator.LESS -> ComparisonOperator.Binary.Less(context.values.bind(literals.first()))
            CottontailGrpc.ComparisonOperator.GEQUAL -> ComparisonOperator.Binary.GreaterEqual(context.values.bind(literals.first()))
            CottontailGrpc.ComparisonOperator.LEQUAL -> ComparisonOperator.Binary.LessEqual(context.values.bind(literals.first()))
            CottontailGrpc.ComparisonOperator.LIKE -> {
                val v = literals.first()
                if (v is StringValue) {
                    ComparisonOperator.Binary.Like(context.values.bind(LikePatternValue.forValue(v.value)))
                } else {
                    throw QueryException.QuerySyntaxException("LIKE operator expects a parsable string value as right operand.")
                }
            }
            CottontailGrpc.ComparisonOperator.MATCH -> {
                val v = literals.first()
                if (v is StringValue) {
                    ComparisonOperator.Binary.Match(context.values.bind(LucenePatternValue(v.value)))
                } else {
                    throw QueryException.QuerySyntaxException("MATCH operator expects a parsable string value as right operand.")
                }
            }
            CottontailGrpc.ComparisonOperator.BETWEEN -> ComparisonOperator.Between(context.values.bind(literals[0]), context.values.bind(literals[1]))
            CottontailGrpc.ComparisonOperator.IN -> ComparisonOperator.In(literals.map { context.values.bind(it) }.toMutableList())
            CottontailGrpc.ComparisonOperator.UNRECOGNIZED -> throw QueryException.QuerySyntaxException("'${operator.name}' is not a valid comparison operator for a boolean predicate!")
        }
    }

    /**
     * Parses and binds the kNN-lookup part of a GRPC [CottontailGrpc.Query]
     *
     * @param input The [OperatorNode.Logical] on which to perform the kNN
     * @param knn The [CottontailGrpc.Knn] object.
     * @param context The [QueryContext] used for query binding.

     * @return The resulting [KnnPredicate].
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseAndBindKnn(input: OperatorNode.Logical, knn: CottontailGrpc.Knn, context: QueryContext): OperatorNode.Logical {
        val columnName = knn.attribute.fqn()
        val column = input.findUniqueColumnForName(columnName)
        val distance = Distances.valueOf(knn.distance.name).kernel
        val hint = knn.hint.toHint()
        val query: Pair<VectorValue<*>, VectorValue<*>?> = when (column.type) {
            is Type.DoubleVector -> Pair(
                knn.query.toDoubleVectorValue(), if (knn.hasWeight()) {
                    /* Filter 1.0 weights. */
                    if (knn.weight.doubleVector.vectorList.all { it == 1.0 }) {
                        null
                    } else {
                        knn.weight.toDoubleVectorValue()
                    }
                } else {
                    null
                }
            )
            is Type.FloatVector -> Pair(
                knn.query.toFloatVectorValue(), if (knn.hasWeight()) {
                    /* Filter 1.0f weights. */
                    if (knn.weight.floatVector.vectorList.all { it == 1.0f }) {
                        null
                    } else {
                        knn.weight.toFloatVectorValue()
                    }
                } else {
                    null
                }
            )
            is Type.LongVector -> Pair(
                knn.query.toLongVectorValue(), if (knn.hasWeight()) {
                    /* Filter 1L weights. */
                    if (knn.weight.longVector.vectorList.all { it == 1L }) {
                        null
                    } else {
                        knn.weight.toLongVectorValue()
                    }
                } else {
                    null
                }
            )
            is Type.IntVector -> Pair(
                knn.query.toIntVectorValue(), if (knn.hasWeight()) {
                    /* Filter 1 weights. */
                    if (knn.weight.intVector.vectorList.all { it == 1 }) {
                        null
                    } else {
                        knn.weight.toIntVectorValue()
                    }
                } else {
                    null
                }
            )
            is Type.BooleanVector -> Pair(
                knn.query.toBooleanVectorValue(), if (knn.hasWeight()) {
                    knn.weight.toBooleanVectorValue()
                } else {
                    null
                }
            )
            is Type.Complex32Vector -> Pair(
                knn.query.toComplex32VectorValue(), if (knn.hasWeight()) {
                    knn.weight.toComplex32VectorValue()
                } else {
                    null
                }
            )
            is Type.Complex64Vector -> Pair(
                knn.query.toComplex64VectorValue(), if (knn.hasWeight()) {
                    knn.weight.toComplex64VectorValue()
                } else {
                    null
                }
            )
            else -> throw QueryException.QuerySyntaxException("A kNN predicate does not contain a valid query vector!")
        }

        /* Generate DistanceLogicalOperatorNode and return it. */
        val predicate = KnnPredicate(column = column, k = knn.k, distance = distance, hint = hint, query = context.values.bind(query.first), query.second?.let { context.values.bind(it) })
        val dist = DistanceLogicalOperatorNode(input, predicate)
        val sort = SortLogicalOperatorNode(dist, arrayOf(KnnUtilities.distanceColumnDef(predicate.column.name.entity()) to SortOrder.ASCENDING))
        return LimitLogicalOperatorNode(sort, predicate.k.toLong(), 0L)
    }

    /**
     * Parses and binds the projection part of a gRPC [CottontailGrpc.Order]-clause
     *
     * @param input The [OperatorNode.Logical] on which to perform projection.
     * @param order The [CottontailGrpc.Order] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [SortLogicalOperatorNode].
     */
    private fun parseAndBindOrder(input: OperatorNode.Logical, order: CottontailGrpc.Order, context: QueryContext): OperatorNode.Logical {
        val sortOn = order.componentsList.map {
            input.findUniqueColumnForName(it.column.fqn()) to SortOrder.valueOf(it.direction.toString())
        }.toTypedArray()
        return SortLogicalOperatorNode(input, sortOn)
    }

    /**
     * Parses and binds the projection part of a gRPC [CottontailGrpc.Query]
     *
     * @param input The [OperatorNode.Logical] on which to perform projection.
     * @param projection The [CottontailGrpc.Projection] object.
     * @param context The [QueryContext] used for query binding.
     *
     * @return The resulting [SelectProjectionLogicalOperatorNode].
     */
    private fun parseAndBindProjection(input: OperatorNode.Logical, projection: CottontailGrpc.Projection, context: QueryContext): OperatorNode.Logical {
        val fields = projection.columnsList.map { p ->
            if (p.hasAlias()) {
                p.column.fqn() to p.alias.fqn()
            } else {
                p.column.fqn() to null
            }
        }
        val type = try {
            Projection.valueOf(projection.op.name)
        } catch (e: java.lang.IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): ${projection.op} is not supported.")
        }

        /* Return logical node expression for projection. */
        return when (type) {
            Projection.SELECT,
            Projection.SELECT_DISTINCT -> SelectProjectionLogicalOperatorNode(input, type, fields)
            Projection.COUNT -> CountProjectionLogicalOperatorNode(input, fields)
            Projection.EXISTS -> ExistsProjectionLogicalOperatorNode(input, fields)
            Projection.SUM,
            Projection.MAX,
            Projection.MIN,
            Projection.MEAN -> AggregatingProjectionLogicalOperatorNode(input, type, fields)
            else -> throw QueryException.QuerySyntaxException("Project of type $type is currently not supported.")
        }
    }

    /**
     * Tries to find and return a [ColumnDef] that matches the given [Name.ColumnName] in
     * this [OperatorNode.Logical]. The match must be unique!
     *
     * @param name [Name.ColumnName] to look for.
     * @return [ColumnDef] that uniquely matches the [Name.ColumnName]
     */
    private fun OperatorNode.Logical.findUniqueColumnForName(name: Name.ColumnName): ColumnDef<*> {
        val candidates = this.findColumnsForName(name)
        if (candidates.isEmpty()) throw QueryException.QueryBindException("Could not find column '$name' in input.")
        if (candidates.size > 1) throw QueryException.QueryBindException("Multiple candidates for column '$name' in input.")
        return candidates.first()
    }

    /**
     * Tries to find and return all [ColumnDef]s that matches the given [Name.ColumnName] in this [OperatorNode.Logical].
     *
     * @param name [Name.ColumnName] to look for.
     * @return List of [ColumnDef] that  match the [Name.ColumnName]
     */
    private fun OperatorNode.Logical.findColumnsForName(name: Name.ColumnName): List<ColumnDef<*>> =
        this.columns.filter { name.matches(it.name) }
}