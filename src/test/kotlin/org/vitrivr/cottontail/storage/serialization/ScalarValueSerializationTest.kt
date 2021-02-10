package org.vitrivr.cottontail.storage.serialization

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value
import java.util.*

/**
 * Test case that tests for correctness of [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class ScalarValueSerializationTest : AbstractSerializationTest() {

    @AfterEach
    fun teardown() = this.cleanup()

    /**
     * Executes the test.
     */
    @RepeatedTest(3)
    fun test() {
        val nameEntity = this.schema.name.entity("longvector-test")
        val idCol = ColumnDef(nameEntity.column("id"), Type.forName("INTEGER"), -1, false)
        val intCol = ColumnDef(nameEntity.column("intCol"), Type.forName("INTEGER"), -1, false)
        val longCol = ColumnDef(nameEntity.column("longCol"), Type.forName("LONG"), -1, false)
        val doubleCol = ColumnDef(nameEntity.column("doubleCol"), Type.forName("DOUBLE"), -1, false)
        val floatCol = ColumnDef(nameEntity.column("floatCol"), Type.forName("FLOAT"), -1, false)
        val byteCol = ColumnDef(nameEntity.column("byteCol"), Type.forName("BYTE"), -1, false)
        val shortCol = ColumnDef(nameEntity.column("shortCol"), Type.forName("SHORT"), -1, false)


        /* Prepare entity. */
        val columns = arrayOf(idCol, intCol, longCol, doubleCol, floatCol, byteCol, shortCol)
        val txn = this.manager.Transaction(TransactionType.USER)
        val schemaTx = this.schema.Tx(txn)
        schemaTx.createEntity(nameEntity, *columns)
        schemaTx.commit()

        /* Load entity. */
        val entity = schemaTx.entityForName(nameEntity)
        val entityTx = entity.Tx(context = txn)

        /* Prepare random number generator. */
        val seed = System.currentTimeMillis()
        val r1 = SplittableRandom(seed)

        /* Insert data into column. */
        var i1 = 1L
        Assertions.assertEquals(0L, entityTx.count())
        repeat(TestConstants.collectionSize) {
            val values: Array<Value?> = arrayOf(
                IntValue(++i1), IntValue(r1.nextInt()), LongValue(r1.nextLong()),
                DoubleValue(r1.nextDouble()), FloatValue(r1.nextDouble()),
                ByteValue(r1.nextInt()), ShortValue(r1.nextInt())
            )
            entityTx.insert(StandaloneRecord(columns = columns, values = values))
        }
        entityTx.commit()

        /* Read data from column. */
        val r2 = SplittableRandom(seed)
        var i2 = 1L
        Assertions.assertEquals(TestConstants.collectionSize.toLong(), entityTx.count())
        repeat(TestConstants.collectionSize) {
            val rec2 = entityTx.read(++i2, columns)
            Assertions.assertEquals(i2, (rec2[idCol] as IntValue).asLong().value) /* Compare IDs. */
            Assertions.assertEquals(r2.nextInt(), (rec2[intCol] as IntValue).value)
            Assertions.assertEquals(r2.nextLong(), (rec2[longCol] as LongValue).value)
            Assertions.assertEquals(r2.nextDouble(), (rec2[doubleCol] as DoubleValue).value)
            Assertions.assertEquals(r2.nextDouble().toFloat(), (rec2[floatCol] as FloatValue).value)
            Assertions.assertEquals(r2.nextInt().toByte(), (rec2[byteCol] as ByteValue).value)
            Assertions.assertEquals(r2.nextInt().toShort(), (rec2[shortCol] as ShortValue).value)
        }

        /* Close all Tx's */
        entityTx.close()
        schemaTx.close()
    }
}