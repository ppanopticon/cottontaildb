package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.enum
import com.google.protobuf.Empty
import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.database.queries.binding.extensions.proto
import org.vitrivr.cottontail.database.queries.binding.extensions.protoFrom
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.grpc.DMLGrpc
import org.vitrivr.cottontail.grpc.TXNGrpc
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.utilities.data.Format
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.time.ExperimentalTime

/**
 * Command to import data into a specified entity in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class ImportDataCommand(
    val ddlStub: DDLGrpc.DDLBlockingStub,
    val dmlStub: DMLGrpc.DMLBlockingStub,
    val txnStub: TXNGrpc.TXNBlockingStub
) : AbstractEntityCommand(name = "import", help = "Used to import data into Cottontail DB.") {

    /** The [Format] used for the import. */
    private val format: Format by option(
        "-f",
        "--format",
        help = "Format for used for data import (Options: ${
            Format.values().joinToString(", ")
        })."
    ).enum<Format>().required()

    /** The [Path] to the input file. */
    private val input: Path by option(
        "-i",
        "--input",
        help = "Limits the amount of printed results"
    ).convert { Paths.get(it) }.required()

    /** Flag indicating, whether the import should be executed in a single transaction or not. */
    private val singleTransaction: Boolean by option("-t", "--transaction").flag()

    override fun exec() {
        /* Read schema and prepare Iterator. */
        val schema = this.readSchema()
        val iterator = this.format.newImporter(this.input, schema)

        /** Begin transaction (if single transaction option has been set). */
        val txId = if (this.singleTransaction) {
            this.txnStub.begin(Empty.getDefaultInstance())
        } else {
            null
        }

        try {
            /* Perform insert. */
            iterator.forEach {
                if (txId != null) {
                    it.txId = txId
                }
                it.from = this.entityName.protoFrom()
                this.dmlStub.insert(it.build())
            }

            /** Commit transaction, if single transaction option has been set. */
            if (txId != null) {
                this.txnStub.commit(txId)
            }
        } catch (e: Throwable) {
            /** Rollback transaction, if single transaction option has been set. */
            if (txId != null) {
                this.txnStub.rollback(txId)
            }
        } finally {
            iterator.close()
        }
    }

    /**
     * Reads the column definitions for the specified schema and returns it.
     *
     * @return List of [ColumnDef] for the current [Name.EntityName]
     */
    private fun readSchema(): Array<ColumnDef<*>> {
        val columns = mutableListOf<ColumnDef<*>>()
        val schemaInfo = this.ddlStub.entityDetails(
            CottontailGrpc.EntityDetailsMessage.newBuilder().setEntity(this.entityName.proto())
                .build()
        )
        schemaInfo.tuplesList.forEach {
            if (it.dataList[1].stringData == "COLUMN") {
                columns.add(
                    ColumnDef(
                        name = Name.ColumnName(
                            *it.dataList[0].stringData.split(Name.NAME_COMPONENT_DELIMITER)
                                .toTypedArray()
                        ),
                        type = Type.forName(it.dataList[2].stringData, it.dataList[4].intData),
                        nullable = it.dataList[5].booleanData
                    )
                )
            }
        }
        return columns.toTypedArray()
    }
}