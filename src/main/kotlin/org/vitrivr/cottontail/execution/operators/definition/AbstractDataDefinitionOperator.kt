package org.vitrivr.cottontail.execution.operators.definition

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
 * @version 1.0.0
 */
@ExperimentalTime
abstract class AbstractDataDefinitionOperator(protected val dboName: Name, protected val action: String) : Operator.SourceOperator() {
    /** The [ColumnDef] produced by this [AbstractDataDefinitionOperator]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
            ColumnDef.withAttributes(Name.ColumnName("action"), "STRING", -1, false),
            ColumnDef.withAttributes(Name.ColumnName("dbo"), "STRING", -1, false),
            ColumnDef.withAttributes(Name.ColumnName("duration_ms"), "DOUBLE", -1, false)
    )

    /**
     * Generates and returns a [StandaloneRecord] for the given [duration]
     */
    fun statusRecord(duration: Duration) = StandaloneRecord(0L, this.columns, arrayOf(StringValue("CREATE ENTITY"), StringValue(dboName.toString()), DoubleValue(duration.inMilliseconds)))
}