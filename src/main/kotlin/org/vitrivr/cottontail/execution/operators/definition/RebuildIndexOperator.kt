package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.SourceOperator] used during query execution. Rebuilds an [Index]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class RebuildIndexOperator(val catalogue: Catalogue, val name: Name.IndexName) : AbstractDataDefinitionOperator(name, "REBUILD INDEX") {
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val catTxn = context.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this.name.entity())) as EntityTx
        val indexTxn = context.getTx(entityTxn.indexForName(this.name)) as IndexTx
        return flow {
            val timedTupleId = measureTimedValue {
                indexTxn.rebuild()
            }
            emit(this@RebuildIndexOperator.statusRecord(timedTupleId.duration))
        }
    }
}