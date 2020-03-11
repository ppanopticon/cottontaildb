package ch.unibas.dmi.dbis.cottontail.utilities.name

/**
 * Formalizes the [Match] between two [Name]s.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
enum class Match {
    EQUAL, /** Two [Name]s are exactly equal (for all [NameType]s). */
    EQUIVALENT, /** Two [Name]s are equivalent (for a pair of a [NameType.FQN] and a [NameType.SIMPLE] [Name]) since they point to the same object in the given context. */
    INCLUDES, /** One [Name] includes the other [Name] (for [NameType.FQN_WILDCARD] and [NameType.WILDCARD]). */
    NO_MATCH; /** Two [Name]  don't match all. */
}