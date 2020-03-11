package ch.unibas.dmi.dbis.cottontail.utilities

import ch.unibas.dmi.dbis.cottontail.utilities.name.Match
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import ch.unibas.dmi.dbis.cottontail.utilities.name.NameType

import org.junit.Test
import org.junit.jupiter.api.Assertions.*
import java.lang.IllegalArgumentException
import java.util.*


/**
 * Some basic unit tests for the [Name] class.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class NameTest {

    @Test
    fun testTyping(){
        /** Test simple name types. */
        assertEquals(NameType.SIMPLE, Name("test").type)
        assertEquals(NameType.SIMPLE, Name("test_very-long-name-with-dashes_and_underscore").type)
        assertEquals(NameType.SIMPLE, Name("abcedea").type)

        /** Test FQN types. */
        assertEquals(NameType.FQN, Name("test.test1").type)
        assertEquals(NameType.FQN, Name("this.is.test2").type)
        assertEquals(NameType.FQN, Name("a.b.c").type)
        assertEquals(NameType.FQN, Name("a.test-c.this_is_a_test").type)

        /** Test wildcard fqn types. */
        assertEquals(NameType.FQN_WILDCARD, Name("test1.test2.*").type)
        assertEquals(NameType.FQN_WILDCARD, Name("this.test3.*").type)
        assertEquals(NameType.FQN_WILDCARD, Name("a.b.*").type)
        assertEquals(NameType.FQN_WILDCARD, Name("a.*").type)

        /** Test wildcard name *. */
        assertEquals(NameType.WILDCARD, Name("*").type)
    }

    @Test
    fun testIllegalNames() {
        /* Illegal characters. */
        assertThrows(IllegalArgumentException().javaClass) {
            Name("this.is!a.1234").type
        }
        assertThrows(IllegalArgumentException().javaClass) {
            Name("this.@tz?.test").type
        }
        assertThrows(IllegalArgumentException().javaClass) {
            Name("this.tz*").type
        }

        /* Empty components. */
        assertThrows(IllegalArgumentException().javaClass) {
            Name("this..1234").type
        }
        assertThrows(IllegalArgumentException().javaClass) {
            Name("this.test.").type
        }

        /* Too long. */
        assertThrows(IllegalArgumentException().javaClass) {
            Name("this.is.a.test.1234").type
        }
    }

    @Test
    fun testAppend() {
        val name1 = Name(UUID.randomUUID().toString())
        val name2 = Name(UUID.randomUUID().toString())
        val name3 = Name(UUID.randomUUID().toString())

        /* Combine name. */
        val name12 = name1.append(name2)
        assertEquals("${name1.name}.${name2.name}", name12.name)

        /* Combine name. */
        val name123 = name12.append(name3)
        assertEquals("${name1.name}.${name2.name}.${name3.name}", name123.name)
        assertNotEquals(name123.append("test"), name123)

        /* Name too long!!! */
        assertThrows(IllegalArgumentException().javaClass) {
            name123.append("test1").append("test2")
        }
    }

    @Test
    fun testMatching() {
        val test1 = Name("test1")
        val test2 = Name("warren.entity.test1")
        val test3 = Name("warren.entity.test2")
        val test4 = Name("*")
        val test5 = Name("warren.entity.*")

        /* Test equality matches. */
        assertEquals(Match.EQUAL, test1.match(test1))
        assertEquals(Match.EQUAL, test2.match(test2))
        assertEquals(Match.EQUAL, test3.match(test3))
        assertEquals(Match.EQUAL, test4.match(test4))
        assertEquals(Match.EQUAL, test5.match(test5))

        /* Test equivalent matches. */
        assertEquals(Match.EQUIVALENT, test1.match(test2))
        assertEquals(Match.EQUIVALENT, test2.match(test1))
        assertNotEquals(Match.EQUIVALENT, test1.match(test1))
        assertNotEquals(Match.EQUIVALENT, test1.match(test3))
        assertNotEquals(Match.EQUIVALENT, test1.match(test4))
        assertNotEquals(Match.EQUIVALENT, test1.match(test5))
        assertNotEquals(Match.EQUIVALENT, test2.match(test2))
        assertNotEquals(Match.EQUIVALENT, test2.match(test3))
        assertNotEquals(Match.EQUIVALENT, test2.match(test4))
        assertNotEquals(Match.EQUIVALENT, test2.match(test5))

        /* Test include matches (FQN Wildcard). */
        assertEquals(Match.INCLUDES, test5.match(test2))
        assertEquals(Match.INCLUDES, test5.match(test3))
        assertEquals(Match.NO_MATCH, test5.match(test1))
        assertEquals(Match.NO_MATCH, test5.match(test4))
        assertEquals(Match.EQUAL, test5.match(test5))

        /* Test include matches (Wildcard). */
        assertEquals(Match.INCLUDES, test4.match(test1))
        assertEquals(Match.INCLUDES, test4.match(test2))
        assertEquals(Match.EQUAL, test4.match(test4))
        assertEquals(Match.NO_MATCH, test4.match(test5))
    }

    @Test
    fun testPrefix() {
        val root = Name(UUID.randomUUID().toString()).append(UUID.randomUUID().toString())
        val names = listOf(root.append("test1"),root.append("test2").append("abc"),root.append("test3"), root)
        val prefix = Name.findLongestCommonPrefix(names)
        assertEquals(root, prefix)
    }

    @Test()
    fun testNormalizationSimple1() {
        val prefix = Name("a.b")
        val fullname = prefix.append("c")
        assertEquals(Name("a.b.c"), fullname)
        assertEquals(Name("c"), fullname.normalize(prefix))
    }

    @Test()
    fun testNormalizationSimple2() {
        val prefix = Name("ab.cd")
        val fullname = prefix.append("ef")
        assertEquals(Name("ab.cd.ef"), fullname)
        assertEquals(Name("ef"), fullname.normalize(prefix))
    }

    @Test()
    fun testNormalizationSimple3() {
        val prefix = Name("abc.def")
        val fullname = prefix.append("ghi")
        assertEquals(Name("abc.def.ghi"), fullname)
        assertEquals(Name("ghi"), fullname.normalize(prefix))
    }
}