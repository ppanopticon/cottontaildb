package org.vitrivr.cottontail.execution.operators.definition

import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.StringValue
import kotlin.time.Duration
import kotlin.time.ExperimentalTime

/**
 * An abstract [Operator.SourceOperator] for execution of DDL statements. Usually processes these
 * statements and returns a status result set.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
@ExperimentalTime
abstract class AbstractDataDefinitionOperator(protected val dboName: Name, protected val action: String) : Operator.SourceOperator() {
    companion object {
        val COLUMNS: Array<ColumnDef<*>> = arrayOf(
            ColumnDef(Name.ColumnName("action"), Type.String, false),
            ColumnDef(Name.ColumnName("dbo"), Type.String, false),
            ColumnDef(Name.ColumnName("duration_ms"), Type.Double, false)
        )
    }

    /** The [ColumnDef] produced by this [AbstractDataDefinitionOperator]. */
    override val columns: Array<ColumnDef<*>> = COLUMNS

    /**
     * Generates and returns a [StandaloneRecord] for the given [duration]
     */
    fun statusRecord(duration: Duration) = StandaloneRecord(
        0L,
        this.columns,
        arrayOf(
            StringValue(this.action),
            StringValue(dboName.toString()),
            DoubleValue(duration.inMilliseconds)
        )
    )
}