package ch.unibas.dmi.dbis.cottontail.storage.store.engine.hare

import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.BufferPool
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.DiskManager
import ch.unibas.dmi.dbis.cottontail.storage.engine.hare.disk.Page

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach

import java.nio.file.Paths
import org.junit.jupiter.api.Assertions.*
import java.nio.ByteBuffer
import java.nio.file.Files
import kotlin.random.Random

class DiskManagerTest {
    val path = Paths.get("./test-db.hare")

    var manager: DiskManager? = null

    val random = Random(System.currentTimeMillis())

    @BeforeEach
    fun beforeEach() {
        DiskManager.init(this.path)
        this.manager = DiskManager(this.path)
    }

    @AfterEach
    fun afterEach() {
        this.manager!!.close()
        Files.delete(this.path)
    }

    @Test
    fun testCreationAndLoading() {
        assertEquals(this.manager!!.pages, 0)
        assertEquals(this.manager!!.size, DiskManager.FILE_HEADER_SIZE_BYTES)
    }

    @Test
    fun testAppendPage() {
        val page = Page(ByteBuffer.allocateDirect(BufferPool.PAGE_MEMORY_SIZE))
        val longs = LongArray(random.nextInt(65536)) {
            random.nextLong()
        }

        for (i in longs.indices) {
            page.putLong(0, longs[i])

            assertTrue(page.dirty)

            this.manager!!.append(page)
            assertEquals(this.manager!!.pages, i+1L)
            assertEquals(this.manager!!.size, DiskManager.FILE_HEADER_SIZE_BYTES + (i+1)*Page.Constants.PAGE_DATA_SIZE_BYTES)
            assertEquals(i.toLong(), page.id)
            assertFalse(page.dirty)
        }

        for (i in longs.indices) {
            this.manager!!.read(i.toLong(), page)
            assertEquals(longs[i], page.getLong(0))
            assertEquals(i.toLong(), page.id)
            assertFalse(page.dirty)
        }
    }
}