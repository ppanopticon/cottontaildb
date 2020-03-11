package ch.unibas.dmi.dbis.cottontail.utilities.name

/**
 * Specifies the type of [Name].
 *
 * @version 1.0
 * @author Ralph Gasser
 */
enum class NameType {
    SIMPLE, /** A simple name with a single component. */
    FQN,  /** A fully qualified name with > 1 components. */
    FQN_WILDCARD,  /** A fully qualified name with > 1 components and a wildcard character (*). */
    WILDCARD /** A wildcard character (*). */
}