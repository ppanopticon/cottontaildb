package org.vitrivr.cottontail.storage.store.engine.hare.views

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.vitrivr.cottontail.model.basics.TupleId
import org.vitrivr.cottontail.storage.engine.hare.Address
import org.vitrivr.cottontail.storage.engine.hare.access.column.directory.DirectoryPageView
import org.vitrivr.cottontail.storage.engine.hare.disk.structures.HarePage
import org.vitrivr.cottontail.storage.engine.hare.views.Flags
import java.nio.ByteBuffer
import kotlin.random.Random

/**
 * Unit tests for [DirectoryPageView].
 *
 * @author Ralph Gasser
 * @version 1.0
 */

class DirectoryPageViewTest {

    private val random = Random(System.currentTimeMillis())


    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun testWrapException(pageSize: Int) {
        val buffer = ByteBuffer.allocate(pageSize)
        val page = HarePage(buffer)
        Assertions.assertThrows(IllegalArgumentException::class.java) {
            DirectoryPageView(page).validate()
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [4096, 8192, 16384, 32768])
    fun test(pageSize: Int) {
        val buffer = ByteBuffer.allocate(pageSize)
        val page = HarePage(buffer)
        DirectoryPageView.initialize(page, -1L, 0L)
        val view = DirectoryPageView(page).validate()
        val list = mutableListOf<Triple<TupleId, Flags, Address>>()
        var last = -1L
        while (!view.full) {
            val flag = this.random.nextInt()
            val address = this.random.nextLong()
            val tupleId = view.allocate(flag, address)
            Assertions.assertEquals(last + 1, tupleId)
            list.add(Triple(tupleId, flag, address))
            last = tupleId
        }

        for (entry in list) {
            Assertions.assertEquals(entry.second, view.getFlags(entry.first))
            Assertions.assertEquals(entry.third, view.getAddress(entry.first))
        }
    }

}