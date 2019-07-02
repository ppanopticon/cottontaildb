package ch.unibas.dmi.dbis.cottontail.calcite.rel

import ch.unibas.dmi.dbis.cottontail.calcite.adapter.CottontailTable
import com.google.common.collect.ImmutableMap
import org.apache.calcite.linq4j.tree.Types
import java.lang.reflect.Method

enum class CottontailMethod(clazz: Class<*>, methodName: String, vararg argumentTypes: Class<*>) {

    COTTONTAIL_QUERYABLE_QUERY(CottontailTable.CottontailTableQueryable::class.java, "query", List::class.java, List::class.java,  List::class.java, Long::class.java, Long::class.java);


    companion object {
        @JvmStatic
        val MAP: Map<Method, CottontailMethod> = ImmutableMap.Builder<Method, CottontailMethod>()
                .put(COTTONTAIL_QUERYABLE_QUERY.method, COTTONTAIL_QUERYABLE_QUERY)
                .build()
    }


    val method: Method = Types.lookupMethod(clazz, methodName, *argumentTypes)
}