package ch.unibas.dmi.dbis.cottontail.utilities.name

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException

/** The separator between Cottontail DB name components. */
const val COTTONTAIL_NAME_COMPONENT_SEPARATOR = '.'

/** Type alias for Cottontail DB names. */
typealias Name = String

/**
 * Returns true if this [Name] is valid given the type.
 *
 * @return true if this [Name] is valid, false otherwise.
 */
internal fun Name.isValid(type: NameType = NameType.FQN): Boolean = when(type) {
    NameType.SIMPLE -> this.matches(Regex("^([a-zA-Z0-9\\-_]+)$"))
    NameType.FQN -> this.matches(Regex("^([a-zA-Z0-9\\-_]+)(\\.[a-zA-Z0-9\\-_]+){0,3}$"))
    NameType.FQN_WILDCARD ->  this.matches(Regex("^([a-zA-Z0-9\\-_]+){1}(\\.([a-zA-Z0-9\\-_]+|\\*)){0,3}\$"))
    NameType.WILDCARD -> this.matches(Regex("^\\*\$"))
}

/**
 * Normalizes the provided name by making it lower case. In the future, normalization might involve further steps.
 */
fun Name.normalize() = this.toLowerCase()

/**
 * Appends the other [Name] to this [Name].
 *
 * @param other The other [Name].
 * @return The concatenated [Name].
 */
internal fun Name.append(other: Name): Name = "$this$COTTONTAIL_NAME_COMPONENT_SEPARATOR$other"

/**
 * Returns the first [Name] component of this [Name], which is a [Name] again. If this is of [NameType.SIMPLE],
 * then the same [Name] is returned.
 *
 * @return Last [Name] component of this [Name]
 */
internal fun Name.first(): Name = this.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR).first()

/**
 * Returns the last [Name] component of this [Name], which is a [Name] again. If this is of [NameType.SIMPLE],
 * then the same [Name] is returned.
 *
 * @return Last [Name] component of this [Name]
 */
internal fun Name.last(): Name = this.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR).last()

/**
 * Returns the [NameType] of this [Name].
 *
 * @return [NameType] of this [Name].
 */
internal fun Name.type(): NameType = when {
    this.contains('.') && this.contains('*') -> NameType.FQN_WILDCARD
    this.contains('.') -> NameType.FQN
    this.contains('*') -> NameType.WILDCARD
    else -> NameType.SIMPLE
}

/**
 * Checks if this [Name] is a prefix of the provided [Name].
 *
 * @param other The [Name] to check.
 */
internal fun Name.isPrefixOf(that: Name): Boolean {
    val o = that.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR)
    val t = this.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR)
    return if (o.size > t.size)
        false
    else
        o.mapIndexed {i,s -> s == t[i]}.all { it }
}

/**
 * Checks if the provided [Name] matches this [Name] in terms of Cottontail DB naming. That is, checks if
 * it is either an exact match (i.e. this == that) or, if this contains a wildcard (*), that matches this.
 *
 * @param that The [String] to check.
 */
internal fun Name.doesNameMatch(that: Name): Boolean {
    val t = that.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR)
    val o = this.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR)
    return if (o.size != t.size)
        false
    else
        o.mapIndexed {i,s -> if (i<o.size-1) {s == t[i]} else {s == t[i] || s == "*"}}.all { it }
}

/**
 * Normalizes this [String] to a column name relative to the given [Entity]. Column names always consist
 * of four components: <instance>.<schema>.<entity>.<column>
 *
 * <strong>Important:</strong> This method does not check, whether the specified column actually exists!
 *
 * @param entity [Entity] relative to which the name should be normalized.
 */
internal fun Name.normalizeColumnName(entity: Entity): String {
    val split = this.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR)
    return when (split.size) {
        1 -> "${entity.fqn}.${split[0]}"
        2 -> "${entity.parent.fqn}.${split[0]}.${split[1]}"
        3 -> "${entity.parent.parent.fqn}.${split[0]}.${split[1]}.${split[2]}"
        4 -> "${split[0]}.${split[1]}.${split[2]}.${split[3]}"
        else -> throw QueryException.QueryBindException("The provided expression '$this' does not point to a column.")
    }
}