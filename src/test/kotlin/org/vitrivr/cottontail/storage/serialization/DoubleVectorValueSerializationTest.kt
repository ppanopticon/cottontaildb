package org.vitrivr.cottontail.storage.serialization

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.database.column.Type
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.utilities.VectorUtility
import java.util.*

/**
 * Test case that tests for correctness of [DoubleVectorValue] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DoubleVectorValueSerializationTest : AbstractSerializationTest() {

    @AfterEach
    fun teardown() = this.cleanup()

    /**
     * Executes the test.
     */
    @ParameterizedTest
    @MethodSource("dimensions")
    fun test(dimension: Int) {
        val nameEntity = this.schema.name.entity("doublevector-test")
        val idCol = ColumnDef(nameEntity.column("id"), Type.forName("INTEGER"), -1, false)
        val vectorCol =
            ColumnDef(nameEntity.column("vector"), Type.forName("DOUBLE_VEC"), dimension, false)

        /* Prepare entity. */
        val columns = arrayOf(idCol, vectorCol)
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
        VectorUtility.randomDoubleVectorSequence(dimension, TestConstants.collectionSize, r1)
            .forEach {
                entityTx.insert(
                    StandaloneRecord(
                        columns = columns,
                        values = arrayOf(IntValue(++i1), it)
                    )
                )
            }
        entityTx.commit()

        /* Read data from column. */
        val r2 = SplittableRandom(seed)
        var i2 = 1L
        VectorUtility.randomDoubleVectorSequence(dimension, TestConstants.collectionSize, r2)
            .forEach {
                val rec2 = entityTx.read(++i2, columns)
                Assertions.assertEquals(
                    i2,
                    (rec2[idCol] as IntValue).asLong().value
                ) /* Compare IDs. */
                Assertions.assertArrayEquals(
                    it.data,
                    (rec2[vectorCol] as DoubleVectorValue).data
                ) /* Compare generated vector and deserialized vector. */
                Assertions.assertFalse(
                    DoubleVectorValue.random(
                        dimension,
                        r1
                    ).data.contentEquals((rec2[vectorCol] as DoubleVectorValue).data)
                ) /* Compare to some random vector and serialized vector; match very unlikely! */
            }

        /* Close all Txs */
        entityTx.close()
        schemaTx.close()
    }
}