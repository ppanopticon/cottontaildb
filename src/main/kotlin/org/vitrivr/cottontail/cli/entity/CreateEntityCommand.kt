package org.vitrivr.cottontail.cli.entity

import com.github.ajalt.clikt.output.TermUi
import com.jakewharton.picnic.table
import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.vitrivr.cottontail.database.queries.binding.extensions.proto
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpc
import org.vitrivr.cottontail.utilities.output.TabulationUtilities
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to create a new a [org.vitrivr.cottontail.database.entity.DefaultEntity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class CreateEntityCommand(private val ddlStub: DDLGrpc.DDLBlockingStub) : AbstractEntityCommand(name = "create", help = "Creates a new entity in the database. Usage: entity create <schema>.<entity>") {

    override fun exec() {
        /* Perform sanity check. */
        checkValid()

        /* Prepare entity definition and prompt user use to add columns to definition. */
        val colDef = CottontailGrpc.EntityDefinition.newBuilder().setEntity(this.entityName.proto())
        do {
            /* Prompt user for column definition. */
            println("Please specify column to add at position ${colDef.columnsCount + 1}.")
            val ret = this.promptForColumn()
            if (ret != null) {
                colDef.addColumns(ret)
            }

            /* Ask if anther column should be added. */
            if (colDef.columnsCount > 0 && TermUi.confirm("Do you want to add another column (size = ${colDef.columnsCount})?") == false) {
                break
            }
        } while (true)

        /* Prepare table for printing. */
        val tbl = table {
            cellStyle {
                border = true
                paddingLeft = 1
                paddingRight = 1
            }
            header {
                row {
                    cell(this@CreateEntityCommand.entityName) {
                        columnSpan = 4
                    }
                }
                row {
                    cells("Name", "Type", "Size", "Nullable")
                }
            }
            body {
                colDef.columnsList.forEach { def ->
                    row {
                        cells(def.name, def.type, def.length, def.nullable)
                    }
                }
            }
        }

        /* As for final confirmation and create entity. */
        if (TermUi.confirm(text = "Please confirm that you want to create the entity:\n$tbl", default = true) == true) {
            val time = measureTimedValue {
                TabulationUtilities.tabulate(this.ddlStub.createEntity(CottontailGrpc.CreateEntityMessage.newBuilder().setDefinition(colDef).build()))
            }
            println("Entity ${this.entityName} created successfully (took ${time.duration}).")
            print(time.value)
        } else {
            println("Create entity ${this.entityName} aborted!")
        }
    }

    /**
     * Checks if this [Name.EntityName] is actually valid to create an [Entity] for.
     *
     * @return true if name is valid, false otherwise.
     */
    private fun checkValid(): Boolean {
        try {
            val ret = this.ddlStub.listEntities(CottontailGrpc.ListEntityMessage.newBuilder().setSchema(this.entityName.schema().proto()).build())
            while (ret.hasNext()) {

                /* ToDo: Check entities.
                if (ret.next().name == this.entityName.simple) {
                    println("Error: Entity ${this.entityName} already exists!")
                    return false
                }*/
            }
            return true
        } catch (e: StatusRuntimeException) {
            if (e.status == Status.NOT_FOUND) {
                println("Error: Schema ${this.entityName.schema()} not found.")
            }
            return false
        }
    }

    /**
     * Prompts user to specify column information and returns a [CottontailGrpc.ColumnDefinition] if user completes this step.
     *
     * @return Optional [CottontailGrpc.ColColumnDefinition]
     */
    private fun promptForColumn(): CottontailGrpc.ColumnDefinition? {
        val def = CottontailGrpc.ColumnDefinition.newBuilder()
        def.name = TermUi.prompt("Column name (must consist of letters and numbers)")
        def.type = TermUi.prompt("Column type (${CottontailGrpc.Type.values().joinToString(", ")})") {
            CottontailGrpc.Type.valueOf(it.toUpperCase())
        }
        def.length = when (def.type) {
            CottontailGrpc.Type.DOUBLE_VEC,
            CottontailGrpc.Type.FLOAT_VEC,
            CottontailGrpc.Type.LONG_VEC,
            CottontailGrpc.Type.INT_VEC,
            CottontailGrpc.Type.BOOL_VEC,
            CottontailGrpc.Type.COMPLEX32_VEC,
            CottontailGrpc.Type.COMPLEX64_VEC -> TermUi.prompt("\rColumn lengths (i.e. number of entries for vectors)") { it.toInt() }
                    ?: 1
            else -> -1
        }
        def.nullable = TermUi.confirm(text = "Should column be nullable?", default = false) ?: false

        val tbl = table {
            cellStyle {
                border = true
                paddingLeft = 1
                paddingRight = 1
            }
            header {
                row {
                    cells("Name", "Type", "Size", "Nullable")
                }
            }
            body {
                row {
                    cells(def.name, def.type, def.length, def.nullable)
                }
            }
        }

        return if (TermUi.confirm("Please confirm column:\n$tbl") == true) {
            def.build()
        } else {
            null
        }
    }
}