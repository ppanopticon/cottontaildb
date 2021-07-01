package org.vitrivr.cottontail.database.queries.planning

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.vitrivr.cottontail.database.AbstractDatabaseTest
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.SelectProjectionLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntitySampleLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform.FetchLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.rules.logical.DeferFetchOnScanRewriteRule
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
/**
 * A collection of test cases for the [DeferFetchOnScanRewriteRule].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DeferFetchOnScanRewriteRuleTest : AbstractDatabaseTest() {

    /** [Name.EntityName] of test entity. */
    private val entityName = this.schemaName.entity("test-entity")

    /** [ColumnDef] in test entity. */
    private val columns = listOf(
        ColumnDef(this.entityName.column("id"), Type.Long),
        ColumnDef(this.entityName.column("doubleValue"), Type.Double),
        ColumnDef(this.entityName.column("stringValue"), Type.Double),
        ColumnDef(this.entityName.column("intValue"), Type.Int),
        ColumnDef(this.entityName.column("booleanValue"), Type.Boolean)
    )

    /** Entities used for this [DeferFetchOnScanRewriteRuleTest]. */
    override val entities: List<Pair<Name.EntityName, List<ColumnDef<*>>>> = listOf(this.entityName to this.columns)

    /**
     * Makes a basic test whether [DeferFetchOnScanRewriteRule.canBeApplied] works as expected and generates output accordingly.
     */
    @Test
    fun testNoMatch() {
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val ctx = QueryContext(txn)
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getTx(schema) as SchemaTx
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = txn.getTx(entity) as EntityTx

            /* Prepare simple SAMPLE with projection. */
            val sample0 = EntitySampleLogicalOperatorNode(0, entityTx, this.columns.toTypedArray(), 10)
            val projection0 = SelectProjectionLogicalOperatorNode(sample0, Projection.SELECT, this.columns.map { it.name to null })

            /* Check DeferFetchOnFetchRewriteRule.canBeApplied and test output for null. */
            Assertions.assertFalse(DeferFetchOnScanRewriteRule.canBeApplied(sample0))
            val result1 = DeferFetchOnScanRewriteRule.apply(sample0, ctx)
            Assertions.assertEquals(null, result1)
        } finally {
            txn.rollback()
        }
    }

    /**
     * Tests case of no deferral because all columns are selected.
     */
    @Test
    fun testNoDefer() {
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val ctx = QueryContext(txn)
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getTx(schema) as SchemaTx
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = txn.getTx(entity) as EntityTx

            /* Prepare simple SAMPLE with projection. */
            val scan0 = EntityScanLogicalOperatorNode(0, entityTx, this.columns.toTypedArray())
            val projection0 = SelectProjectionLogicalOperatorNode(scan0, Projection.SELECT, this.columns.map { it.name to null })

            /* Step 1: Execute DeferFetchOnScanRewriteRule and make basic assertions. */
            Assertions.assertTrue(DeferFetchOnScanRewriteRule.canBeApplied(scan0))
            val result1 = DeferFetchOnScanRewriteRule.apply(scan0, ctx)

            /* Output should be null because no deferral can take place. */
            Assertions.assertEquals(null, result1)
        } finally {
            txn.rollback()
        }
    }

    /**
     * Tests simple deferral and dropping of columns in a SELECT followed by a PROJECTION
     */
    @Test
    fun testDeferAndDrop() {
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val ctx = QueryContext(txn)
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getTx(schema) as SchemaTx
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = txn.getTx(entity) as EntityTx

            /* Prepare simple scan with projection. */
            val scan0 = EntityScanLogicalOperatorNode(0, entityTx, this.columns.toTypedArray())
            val projection0 = SelectProjectionLogicalOperatorNode(scan0, Projection.SELECT, listOf(this.columns[0].name to null, this.columns[1].name to null))

            /* Execute rule, */
            Assertions.assertTrue(DeferFetchOnScanRewriteRule.canBeApplied(scan0))
            val result1 = DeferFetchOnScanRewriteRule.apply(scan0, ctx)

            /* Check order: SCAN -> PROJECT. */
            Assertions.assertTrue(result1 is SelectProjectionLogicalOperatorNode)
            val scan1 = (result1 as SelectProjectionLogicalOperatorNode).input
            Assertions.assertTrue(scan1 is EntityScanLogicalOperatorNode)

            /* Check that columns are preserved and unnecessary columns are dropped. */
            Assertions.assertTrue(result1.columns.contentDeepEquals(projection0.columns)) /* Columns of the resulting PROJECTION should be the same as the original PROJECTION */
            Assertions.assertTrue((result1.input as EntityScanLogicalOperatorNode).columns.contentDeepEquals(projection0.columns)) /* Columns SCANNED should only contain the projected columns. */
        } finally {
            txn.rollback()
        }
    }

    /**
     * Tests simple deferral and dropping of columns in a SELECT followed by a PROJECTION
     */
    @Test
    fun testDeferAfterFilter() {
        val txn = this.manager.Transaction(TransactionType.SYSTEM)
        try {
            val ctx = QueryContext(txn)
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getTx(schema) as SchemaTx
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = txn.getTx(entity) as EntityTx

            /* Prepare simple SCAN followed by a FILTER, followed by a PROJECTION. */
            val scan0 = EntityScanLogicalOperatorNode(0, entityTx, this.columns.toTypedArray())
            val filter0 = FilterLogicalOperatorNode(scan0, BooleanPredicate.Atomic.Literal(this.columns[2], ComparisonOperator.Binary.Equal(Binding(0)), false))
            val projection0 = SelectProjectionLogicalOperatorNode(filter0, Projection.SELECT, listOf(this.columns[0].name to null, this.columns[1].name to null))


            /* Execute rule and make basic assertions. */
            Assertions.assertTrue(DeferFetchOnScanRewriteRule.canBeApplied(scan0))
            val result1 = DeferFetchOnScanRewriteRule.apply(scan0, ctx)

            /* Check order: SCAN -> FILTER -> FETCH -> PROJECT. */
            Assertions.assertTrue(result1 is SelectProjectionLogicalOperatorNode)
            Assertions.assertTrue(result1!!.columns.contentDeepEquals(projection0.columns))
            val fetch1 = (result1 as SelectProjectionLogicalOperatorNode).input
            Assertions.assertTrue(fetch1 is FetchLogicalOperatorNode)
            val filter1 = (fetch1 as FetchLogicalOperatorNode).input
            Assertions.assertTrue(filter1 is FilterLogicalOperatorNode)
            val scan1 = (filter1 as FilterLogicalOperatorNode).input
            Assertions.assertTrue(scan1 is EntityScanLogicalOperatorNode)

            /* Check that columns are preserved and unnecessary columns are dropped. */
            Assertions.assertTrue(result1.columns.contentDeepEquals(projection0.columns)) /* Columns of the resulting PROJECTION should be the same as the original PROJECTION */

            val combined = ((scan1 as EntityScanLogicalOperatorNode).columns + fetch1.fetch) /* Columns of the SCAN + FETCH. */
            Assertions.assertEquals(scan0.columns.size, combined.size) /* Columns FETCHED + columns SCANNED should of the same size as the SCAN columns. */
            Assertions.assertTrue(scan1.columns.all { combined.contains(it) }) /* Columns FETCHED + columns SCANNED should be all columns in the original SCAN. */
            Assertions.assertTrue(scan1.columns.contentDeepEquals(filter0.predicate.columns.toTypedArray())) /* Columns SCANNED should only contain the columns used by FILTER. */
        } finally {
            txn.rollback()
        }
    }


    override fun populateDatabase() {/* No op. */ }
}