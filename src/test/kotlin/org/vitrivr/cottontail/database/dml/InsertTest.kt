package org.vitrivr.cottontail.database.dml

import org.apache.commons.lang3.RandomStringUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value
import java.nio.file.Files
import java.util.*
import java.util.stream.Collectors

class InsertTest {
    private val schemaName = Name.SchemaName("test")
    private val entityName = Name.EntityName("test", "one")
    private val idColumn = ColumnDef.withAttributes(this.entityName.column("id"), "STRING")
    private val featureColumn = ColumnDef.withAttributes(this.entityName.column("feature"), "STRING")

    /** */
    private var catalogue: Catalogue = Catalogue(TestConstants.config)

    private var schema: Schema? = null


    @BeforeEach
    fun initialize() {
        if (this.catalogue.schemas.contains(schemaName)) {
            this.catalogue.dropSchema(schemaName)
        }
        this.catalogue.createSchema(schemaName)
        this.schema = this.catalogue.schemaForName(schemaName)
    }

    @AfterEach
    fun teardown() {
        this.catalogue.close()
        val pathsToDelete = Files.walk(TestConstants.config.root).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
        pathsToDelete.forEach { Files.delete(it) }
    }

    fun createLuceneEntity() {
        schema?.createEntity(entityName, idColumn, featureColumn)
        schema?.entityForName(entityName).createIndex(entityName.index("lucene-idx"), IndexType.LUCENE, arrayOf(ColumnDef.withAttributes(entityName.column("feature"), "STRING")))
    }

    private fun dropLuceneEntity() {
        schema?.dropEntity(entityName)
    }

    private fun generateRecord(): Record {
        val columns = ArrayList<ColumnDef<*>>(2)
        val values = ArrayList<Value?>(2)

        columns.add(idColumn)
        columns.add(featureColumn)
        values.add(StringValue(RandomStringUtils.randomAlphanumeric(10)))
        values.add(StringValue(RandomStringUtils.randomAlphabetic(20)))
        return StandaloneRecord(columns = columns.toTypedArray(), values = values.toTypedArray())
    }

    private fun insertRandomRecord() {
        val tx = schema?.entityForName(entityName).Tx(false)
        tx.use {
            tx.insert(generateRecord())
            tx.commit()
            tx.close()
        }
    }

    @Test
    fun insertIntoEntity() {
        createLuceneEntity()
        insertRandomRecord()
    }

    @Test
    fun insertDrop() {
        insertIntoEntity()
        dropLuceneEntity()
    }

    @Test
    fun insertTwiceIntoEntity() {
        createLuceneEntity()
        insertRandomRecord()
        insertRandomRecord()
    }

    @Test
    fun insertTwiceDrop() {
        insertTwiceIntoEntity()
        dropLuceneEntity()
    }

    @Test
    fun insertOptimize() {
        createLuceneEntity()
        insertRandomRecord()
        insertRandomRecord()
        schema?.entityForName(entityName).updateAllIndexes()
    }

}
