package ch.unibas.dmi.dbis.cottontail.server.grpc.helper

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.database.queries.*

import ch.unibas.dmi.dbis.cottontail.execution.ExecutionEngine
import ch.unibas.dmi.dbis.cottontail.execution.ExecutionPlan
import ch.unibas.dmi.dbis.cottontail.execution.ExecutionPlanFactory
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask

import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc

import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.Distance

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.type.*
import ch.unibas.dmi.dbis.cottontail.model.values.Value
import ch.unibas.dmi.dbis.cottontail.utilities.name.doesNameMatch
import ch.unibas.dmi.dbis.cottontail.utilities.name.normalizeColumnName

/**
 * This helper class parses and binds queries issued through the GRPC endpoint. The process encompasses three steps:
 *
 * 1) The [CottontailGrpc.Query] is decomposed into its components.
 * 2) The GRPC query components are bound to Cottontail DB [DBO] objects and internal query objects are constructed. This step includes some basic validation.
 * 3) A [ExecutionPlan] is constructed from the internal query objects.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class GrpcQueryBinder(val catalogue: Catalogue, engine: ExecutionEngine) {

    /** [ExecutionPlanFactor] used to generate [ExecutionPlan]s from query definitions. */
    private val factory = ExecutionPlanFactory(engine)

    /**
     * Binds the given [CottontailGrpc.Query] to the database objects and thereby creates an [ExecutionPlan].
     *
     * @param query The [CottontailGrpc.Query] that should be bound.
     * @return [ExecutionPlan]
     *
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     * @throws QueryException.QuerySyntaxException If [CottontailGrpc.Query] is structurally incorrect.
     */
    fun parseAndBind(query: CottontailGrpc.Query): ExecutionPlan{
        if (!query.hasFrom()) throw QueryException.QuerySyntaxException("The query lacks a valid FROM-clause.")
        return when {
            query.from.hasEntity() -> parseAndBindSimpleQuery(query)
            else -> throw QueryException.QuerySyntaxException("The query lacks a valid FROM-clause.")
        }
    }

    /**
     * Parses and binds a simple [CottontailGrpc.Query] without any joins and thereby generates an [ExecutionPlan].
     *
     * @param query The simple [CottontailGrpc.Query] object.
     */
    private fun parseAndBindSimpleQuery(query: CottontailGrpc.Query): ExecutionPlan {
        val entity = try {
            this.catalogue.schemaForName(query.from.entity.schema.name).entityForName(query.from.entity.name)
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${query.from.entity.fqn()}'. Schema does not exist!")
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            throw QueryException.QueryBindException("Failed to bind '${query.from.entity.fqn()}'. Entity does not exist!")
        } catch (e: DatabaseException) {
            throw QueryException.QueryBindException("Failed to bind ${query.from.entity.fqn()}. Database error!")
        }

        /* Create projection clause. */
        val projectionClause = if (query.hasProjection()) {
            parseAndBindProjection(entity, query.projection)
        } else {
            parseAndBindProjection(entity, CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.Operation.SELECT).putAttributes("*","").build())
        }
        val knnClause = if (query.hasKnn()) parseAndBindKnnPredicate(entity, query.knn) else null
        val whereClause = if (query.hasWhere()) parseAndBindBooleanPredicate(entity, query.where) else null

        /* Transform to ExecutionPlan. */
        return this.factory.simpleExecutionPlan(entity, projectionClause, knnClause = knnClause, whereClause = whereClause, limit = query.limit, skip = query.skip)
    }

    /**
     * Parses and binds a [CottontailGrpc.Where] clause.
     *
     * @param entity The [Entity] from which fetch columns.
     * @param where The [CottontailGrpc.Where] object.
     *
     * @return The resulting [AtomicBooleanPredicate].
     */
    private fun parseAndBindBooleanPredicate(entity: Entity, where: CottontailGrpc.Where): BooleanPredicate =  when(where.predicateCase) {
        CottontailGrpc.Where.PredicateCase.ATOMIC ->  parseAndBindAtomicBooleanPredicate(entity, where.atomic)
        CottontailGrpc.Where.PredicateCase.COMPOUND ->  parseAndBindCompoundBooleanPredicate(entity, where.compound)
        CottontailGrpc.Where.PredicateCase.PREDICATE_NOT_SET -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
        null -> throw QueryException.QuerySyntaxException("WHERE clause without a predicate is invalid!")
    }

    /**
     * Parses and binds an atomic boolean predicate
     *
     * @param entity The [Entity] from which fetch columns.
     * @param projection The [CottontailGrpc.Knn] object.
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
     * @param projection The [CottontailGrpc.Knn] object.
     *
     * @return The resulting [AtomicBooleanPredicate].
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseAndBindAtomicBooleanPredicate(entity: Entity, atomic: CottontailGrpc.AtomicLiteralBooleanPredicate): AtomicBooleanPredicate {
        val column = entity.columnForName(atomic.attribute) ?: throw QueryException.QueryBindException("Failed to bind column '${atomic.attribute}'. Column does not exist on entity '${entity.fqn}'.")
        val operator = try {
            ComparisonOperator.valueOf(atomic.op.name)
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("'${atomic.op.name}' is not a valid comparison operator for a boolean predicate!")
        }

        /* Perform some sanity checks. */
        when {
            operator == ComparisonOperator.LIKE && !entity.hasIndexForColumn(column, IndexType.LUCENE) -> throw QueryException.QueryBindException("Failed to bind query '${atomic.attribute} LIKE :1' on entity '${entity.fqn}'. The entity does not have a text-index on the specified column '${column.name}', which is required for LIKE comparisons.")
        }

        /* Return the resulting AtomicBooleanPredicate. */
        return when (column.type) {
            is DoubleType -> AtomicBooleanPredicate(column = column as ColumnDef<Value<Double>>, operator = operator,  values = atomic.dataList.map { it.toDoubleValue() ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.")})
            is FloatType -> AtomicBooleanPredicate(column = column as ColumnDef<Value<Float>>, operator = operator, values = atomic.dataList.map { it.toFloatValue() ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.") })
            is LongType -> AtomicBooleanPredicate(column = column as ColumnDef<Value<Long>>, operator = operator, values = atomic.dataList.map { it.toLongValue() ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.") })
            is IntType -> AtomicBooleanPredicate(column = column as ColumnDef<Value<Int>>, operator = operator, values = atomic.dataList.map { it.toIntValue() ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.") })
            is ShortType -> AtomicBooleanPredicate(column = column as ColumnDef<Value<Short>>, operator = operator, values = atomic.dataList.map { it.toShortValue() ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.") })
            is ByteType -> AtomicBooleanPredicate(column = column as ColumnDef<Value<Byte>>, operator = operator,  values = atomic.dataList.map { it.toByteValue() ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.") })
            is BooleanType -> AtomicBooleanPredicate(column = column as ColumnDef<Value<Boolean>>, operator = operator, values = atomic.dataList.map { it.toBooleanValue() ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.") })
            is StringType -> AtomicBooleanPredicate(column = column as ColumnDef<Value<String>>, operator = operator, values = atomic.dataList.map { it.toStringValue() ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.") })
            is FloatArrayType -> AtomicBooleanPredicate(column = column as ColumnDef<Value<FloatArray>>, operator = operator, values = atomic.dataList.map { it.toFloatVectorValue() ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.") })
            is DoubleArrayType -> AtomicBooleanPredicate(column = column as ColumnDef<Value<DoubleArray>>, operator = operator, values = atomic.dataList.map { it.toDoubleVectorValue() ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.") })
            is LongArrayType -> AtomicBooleanPredicate(column = column as ColumnDef<Value<LongArray>>, operator = operator, values = atomic.dataList.map { it.toLongVectorValue() ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.") })
            is IntArrayType -> AtomicBooleanPredicate(column = column as ColumnDef<Value<IntArray>>, operator = operator, values = atomic.dataList.map { it.toIntVectorValue() ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.") })
            is BooleanArrayType -> AtomicBooleanPredicate(column = column as ColumnDef<Value<BooleanArray>>, operator = operator, values = atomic.dataList.map { it.toBooleanVectorValue() ?: throw QueryException.QuerySyntaxException("Cannot compare ${column.name} to NULL value with operator $operator.") })
        }
    }

    /**
     * Parses and binds the kNN-lookup part of a GRPC [CottontailGrpc.Query]
     *
     * @param entity The [Entity] from which fetch columns.
     * @param projection The [CottontailGrpc.Knn] object.
     *
     * @return The resulting [ExecutionTask].
     */
    @Suppress("UNCHECKED_CAST")
    private fun parseAndBindKnnPredicate(entity: Entity, knn: CottontailGrpc.Knn): KnnPredicate<*> {
        val column = entity.columnForName(knn.attribute) ?: throw QueryException.QueryBindException("Failed to bind column '${knn.attribute}'. Column does not exist on entity '${entity.fqn}'!")

        /* Extracts the query vector. */
        val query: List<Array<Number>> = knn.queryList.map { q ->
            when (q.vectorDataCase) {
                CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> q.floatVector.vectorList.toTypedArray() as Array<Number>
                CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> q.doubleVector.vectorList.toTypedArray() as Array<Number>
                CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> q.intVector.vectorList.toTypedArray() as Array<Number>
                CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> q.longVector.vectorList.toTypedArray() as Array<Number>
                CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.QuerySyntaxException("A kNN predicate does not contain a valid query vector!")
                null -> throw QueryException.QuerySyntaxException("A kNN predicate does not contain a valid query vector!")
            }
        }

        /* Extracts the query vector. */
        val weights: List<Array<Number>>? = if (knn.weightsCount > 0) {
            knn.weightsList.map { w ->
                when (w.vectorDataCase) {
                    CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> w.floatVector.vectorList.toTypedArray() as Array<Number>
                    CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> w.doubleVector.vectorList.toTypedArray() as Array<Number>
                    CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> w.intVector.vectorList.toTypedArray() as Array<Number>
                    CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> w.longVector.vectorList.toTypedArray() as Array<Number>
                    CottontailGrpc.Vector.VectorDataCase.VECTORDATA_NOT_SET -> throw QueryException.QuerySyntaxException("A kNN predicate does not contain a valid weight vector!")
                    null -> throw QueryException.QuerySyntaxException("A kNN predicate does not contain a valid weight vector!")
                }
            }
        } else {
            null
        }

        /* Generate the predicate. */
        return try {
            KnnPredicate(column = column, k = knn.k, query = query, weights = weights, distance = Distance.valueOf(knn.distance.name))
        } catch (e: IllegalArgumentException) {
            throw QueryException.QuerySyntaxException("The '${knn.distance}' is not a valid distance function for a kNN predicate.")
        }
    }

    /**
     * Parses and binds the projection part of a GRPC [CottontailGrpc.Query]
     *
     * @param involvedEntities The list of [Entity] objects involved in the projection.
     * @param projection The [CottontailGrpc.Projection] object.
     *
     * @return The resulting [Projection].
     */
    private fun parseAndBindProjection(entity: Entity, projection: CottontailGrpc.Projection): Projection = try {
        val availableColumns = entity.allColumns()
        val requestedColumns = mutableListOf<ColumnDef<*>>()

        val fields = projection.attributesMap.map { (expr, alias) ->
            /* Fetch columns that match field and add them to list of requested columns */
            val field = expr.normalizeColumnName(entity)
            availableColumns.filter { field.doesNameMatch(it.name) }.let { requestedColumns.addAll(it) }

            /* Return field to alias mapping. */
            field to if (alias.isEmpty()) { null } else { alias }
        }.toMap()

        Projection(type = ProjectionType.valueOf(projection.op.name), columns = requestedColumns.distinct().toTypedArray(), fields = fields)
    } catch (e: java.lang.IllegalArgumentException) {
        throw QueryException.QuerySyntaxException("The query lacks a valid SELECT-clause (projection): ${projection.op} is not supported.")
    }
}