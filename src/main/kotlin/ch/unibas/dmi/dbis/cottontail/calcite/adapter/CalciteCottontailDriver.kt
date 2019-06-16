package ch.unibas.dmi.dbis.cottontail.calcite.adapter

import ch.unibas.dmi.dbis.cottontail.Cottontail
import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.config.CalciteConnectionProperty
import org.apache.calcite.config.Lex
import java.sql.SQLException
import org.apache.calcite.jdbc.CalciteConnection
import org.apache.calcite.jdbc.Driver
import org.apache.calcite.sql.parser.impl.SqlParserImplConstants.DOUBLE_QUOTE
import java.sql.Connection
import java.util.*


/**
 * JDBC driver for Calcite based access to Cottontail DB .
 *
 * It accepts connect strings that start with "jdbc:cottontail:".
 */
class CalciteCottontailDriver private constructor() : Driver() {

    /**
     *
     */
    companion object {
        const val CONNECT_STRING_PREFIX = "jdbc:cottontail:"
        init {
            CalciteCottontailDriver().register()
        }
    }

    /**
     * Returns the JDBC connection string prefix for Cottontail DB driver.
     *
     * @return JDBC connection string prefix
     */
    override fun getConnectStringPrefix(): String = CONNECT_STRING_PREFIX

    /**
     *
     */
    @Throws(SQLException::class)
    override fun connect(url: String, info: Properties): Connection? {
        if (!this.acceptsURL(url)) {
            return null
        }

        /* Hard-codes the schema factory for this Driver implementation. */
        info[CalciteConnectionProperty.SCHEMA_FACTORY.camelName()] = CottontailCatalogueFactory::class.java.name
        info[CalciteConnectionProperty.QUOTING.camelName()] = "DOUBLE_QUOTE"
        info[CalciteConnectionProperty.UNQUOTED_CASING.camelName()] = Casing.TO_LOWER.toString()
        info[CalciteConnectionProperty.QUOTED_CASING.camelName()] = Casing.TO_LOWER.toString()
        info[CalciteConnectionProperty.CASE_SENSITIVE.camelName()] = "false"

        /* Initialize the connection. */
        return super.connect(url, info) as CalciteConnection
    }
}

