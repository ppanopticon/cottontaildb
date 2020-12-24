package org.vitrivr.cottontail.storage.serialization

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest

import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value

import java.util.*

/**
 * Test case that tests for correctness of [LongVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class ScalarValueSerializationTest : AbstractSerializationTest() {

    @AfterEach
    fun teardown() = this.cleanup()

    /**
     * Executes the test.
     */
    @RepeatedTest(3)
    fun test() {
        val nameEntity = this.schemaName.entity("longvector-test")
        val idCol = ColumnDef(nameEntity.column("id"), ColumnType.forName("INTEGER"), -1, false)
        val intCol = ColumnDef(nameEntity.column("intCol"), ColumnType.forName("INTEGER"), -1, false)
        val longCol = ColumnDef(nameEntity.column("longCol"), ColumnType.forName("LONG"), -1, false)
        val doubleCol = ColumnDef(nameEntity.column("doubleCol"), ColumnType.forName("DOUBLE"), -1, false)
        val floatCol = ColumnDef(nameEntity.column("floatCol"), ColumnType.forName("FLOAT"), -1, false)
        val byteCol = ColumnDef(nameEntity.column("byteCol"), ColumnType.forName("BYTE"), -1, false)
        val shortCol = ColumnDef(nameEntity.column("shortCol"), ColumnType.forName("SHORT"), -1, false)


        /* Prepare entity. */
        val columns = arrayOf(idCol, intCol, longCol, doubleCol, floatCol, byteCol, shortCol)
        this.catalogue.createEntity(nameEntity, *columns)
        val entity = this.catalogue.instantiateEntity(nameEntity)

        /* Prepare random number generator. */
        val seed = System.currentTimeMillis()
        val r1 = SplittableRandom(seed)

        /* Insert data into column. */
        entity.Tx(false).begin { tx1 ->
            var i1 = 1L
            Assertions.assertEquals(0L, tx1.count())
            repeat(TestConstants.collectionSize) {
                val values: Array<Value?> = arrayOf(
                        IntValue(i1++), IntValue(r1.nextInt()), LongValue(r1.nextLong()),
                        DoubleValue(r1.nextDouble()), FloatValue(r1.nextDouble()),
                        ByteValue(r1.nextInt()), ShortValue(r1.nextInt())
                )
                tx1.insert(StandaloneRecord(columns = columns, values = values))
            }
            true
        }

        /* Read data from column. */
        val r2 = SplittableRandom(seed)
        entity.Tx(true).begin { tx2 ->
            var i2 = 1L
            Assertions.assertEquals(TestConstants.collectionSize.toLong(), tx2.count())
            repeat(TestConstants.collectionSize) {
                val rec2 = tx2.read(i2 - 1, columns)
                Assertions.assertEquals(i2++, (rec2[idCol] as IntValue).asLong().value) /* Compare IDs. */
                Assertions.assertEquals(r2.nextInt(), (rec2[intCol] as IntValue).value)
                Assertions.assertEquals(r2.nextLong(), (rec2[longCol] as LongValue).value)
                Assertions.assertEquals(r2.nextDouble(), (rec2[doubleCol] as DoubleValue).value)
                Assertions.assertEquals(r2.nextDouble().toFloat(), (rec2[floatCol] as FloatValue).value)
                Assertions.assertEquals(r2.nextInt().toByte(), (rec2[byteCol] as ByteValue).value)
                Assertions.assertEquals(r2.nextInt().toShort(), (rec2[shortCol] as ShortValue).value)
            }
            true
        }
    }
}