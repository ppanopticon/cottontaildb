package org.vitrivr.cottontail.server.grpc

import org.apache.commons.lang3.RandomStringUtils
import org.vitrivr.cottontail.TestConstants
import org.vitrivr.cottontail.TestConstants.INT_COLUMN_NAME
import org.vitrivr.cottontail.TestConstants.STRING_COLUMN_NAME
import org.vitrivr.cottontail.TestConstants.TEST_ENTITY_FQN_INPUT
import org.vitrivr.cottontail.TestConstants.TEST_ENTITY_TUPLE_COUNT
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.ddl.CreateEntity
import org.vitrivr.cottontail.client.language.ddl.CreateSchema
import org.vitrivr.cottontail.client.language.ddl.DropSchema
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.client.stub.SimpleClient
import kotlin.random.Random

fun dropTestSchema(client: SimpleClient) {
    /* Teardown */
    try {
        client.drop(DropSchema(TestConstants.TEST_SCHEMA))
    } catch (e: Exception) {
        //ignore
    }
}

fun createTestSchema(client: SimpleClient) {
    client.create(CreateSchema(TestConstants.TEST_SCHEMA))
}

fun createTestEntity(client: SimpleClient) {
    val create = CreateEntity(TEST_ENTITY_FQN_INPUT).column(STRING_COLUMN_NAME, Type.STRING).column(INT_COLUMN_NAME, Type.INTEGER)
    client.create(create)
}

fun populateTestEntity(client: SimpleClient) {
    val batch = BatchInsert().into(TEST_ENTITY_FQN_INPUT).columns(STRING_COLUMN_NAME, INT_COLUMN_NAME)
    val random = Random.Default
    repeat(TEST_ENTITY_TUPLE_COUNT.toInt()) {
        batch.append(RandomStringUtils.randomAlphabetic(5), random.nextInt(0, 10))
    }
    client.insert(batch)
}