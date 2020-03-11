package ch.unibas.dmi.dbis.cottontail.utilities.name

/**
 * A [Name] that identifies a DBO used within Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class Name(name: String) {

    companion object {

        /** The separator between Cottontail DB name components. */
        const val COTTONTAIL_NAME_COMPONENT_SEPARATOR = '.'

        /** The separator between Cottontail DB name components. */
        const val COTTONTAIL_NAME_COMPONENT_ROOT = "warren"

        /** [Regex] to match [NameType.SIMPLE]*/
        val SIMPLE_NAME_REGEX = Regex("^([a-zA-Z0-9\\-_]+)$")

        /** [Regex] to match [NameType.FQN]*/
        val FQN_NAME_REGEX = Regex("^([a-zA-Z0-9\\-_]+)(\\.[a-zA-Z0-9\\-_]+){0,3}$")

        /** [Regex] to match [NameType.FQN_WILDCARD]*/
        val FQN_WILDCARD_NAME_REGEX = Regex("^([a-zA-Z0-9\\-_]+){1}(\\.([a-zA-Z0-9\\-_]+|\\*)){0,3}\$")

        /** [Regex] to match [NameType.FQN_WILDCARD]*/
        val WILDCARD_NAME_REGEX = Regex("^\\*\$")

        /** The separator between Cottontail DB name components. */
        val COTTONTAIL_ROOT_NAME = Name("warren")

        /**
         * Finds the longest, common prefix the [Name]s in the given collection share.
         *
         * @param names Collection of [Name]s
         * @return Longest, common prefix as [Name]
         */
        fun findLongestCommonPrefix(names: Collection<Name>) : Name {
            val prefix = Array<String?>(3) { null }
            for (i in 0 until 3) {
                val d = names.mapNotNull {
                    val split = it.name.split('.')
                    if (i < split.size) {
                        split[i]
                    } else {
                        null
                    }
                }.distinct()
                if (d.size == 1) {
                    prefix[i] = d.first()
                } else {
                    break
                }
            }
            return Name(prefix.filterNotNull().joinToString (separator = "."))
        }
    }

    /** Cottontail DB [Name]s are always lower-case values. */
    val name = name.toLowerCase()

    /** The [NameType] of this [Name]. */
    val type: NameType = when {
        this.name.matches(SIMPLE_NAME_REGEX) -> NameType.SIMPLE
        this.name.matches(FQN_NAME_REGEX) -> NameType.FQN
        this.name.matches(FQN_WILDCARD_NAME_REGEX) -> NameType.FQN_WILDCARD
        this.name.matches(WILDCARD_NAME_REGEX) -> NameType.WILDCARD
        else -> throw IllegalArgumentException("The provided name {$name} does not match any of the supported name types.")
    }

    /**
     * Appends the other [Name] to this [Name].
     *
     * @param other The other [Name].
     * @return The concatenated [Name].
     */
    fun append(other: Name): Name = Name("$this$COTTONTAIL_NAME_COMPONENT_SEPARATOR${other.name}")

    /**
     * Appends the other name component to this [Name].
     *
     * @param other The other name component.
     * @return The concatenated [Name].
     */
    fun append(other: String): Name = Name("$this$COTTONTAIL_NAME_COMPONENT_SEPARATOR$other")

    /**
     * Returns the first [Name] component of this [Name], which is a [Name] again. If this is of [NameType.SIMPLE],
     * then the same [Name] is returned.
     *
     * @return Last [Name] component of this [Name]
     */
    fun first(): Name = Name(this.name.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR).first())

    /**
     * Returns the last [Name] component of this [Name], which is a [Name] again. If this is of [NameType.SIMPLE],
     * then the same [Name] is returned.
     *
     * @return Last [Name] component of this [Name]
     */
    fun last(): Name = Name(this.name.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR).last())

    /**
     * Returns true, if this [Name] is a root name (i.e. equals to the [COTTONTAIL_ROOT_NAME]).
     *
     * @return True if this [Name] is a root name.
     */
    fun isRoot(): Boolean = this.name == COTTONTAIL_NAME_COMPONENT_ROOT

    /**
     * Checks if this [Name] is a prefix of the provided [Name].
     *
     * @param that The [Name] to check.
     */
    fun isPrefixOf(that: Name): Boolean {
        val o = that.name.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR)
        val t = this.name.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR)
        return if (o.size > t.size)
            false
        else
            o.mapIndexed {i,s -> s == t[i]}.all { it }
    }

    /**
     * Returns the [Match] between two [Name]. The rules of matching are as follows:
     *
     * - Two [Name]s match as [Match.EQUAL] if they are exactly equal.
     * - A [NameType.SIMPLE] and a [NameType.FQN] match as [Match.EQUIVALENT], if their last component is equal.
     * - A [NameType.FQN_WILDCARD] matches [NameType.SIMPLE] and [NameType.FQN] as [Match.INCLUDES], if they share a common prefix.
     * - A [NameType.WILDCARD] matches all [NameType.SIMPLE] and [NameType.FQN] as [Match.INCLUDES]
     *
     * If none of the above is true, two [Name]s don't match at all (i.e. [Match.NO_MATCH]).
     */
    fun match(that: Name): Match = when (this.type) {
        NameType.FQN -> when (that.type) {
            NameType.FQN -> if (this.name == that.name) Match.EQUAL else Match.NO_MATCH
            NameType.SIMPLE -> if (this.last().name == that.name) Match.EQUIVALENT else Match.NO_MATCH
            NameType.FQN_WILDCARD -> if (this.name.startsWith(that.name.subSequence(0..that.name.length-3))) Match.INCLUDES else Match.NO_MATCH
            NameType.WILDCARD -> Match.INCLUDES
        }
        NameType.SIMPLE -> when (that.type) {
            NameType.FQN -> if(this.name == that.last().name) Match.EQUIVALENT else Match.NO_MATCH
            NameType.SIMPLE -> if(this.name == that.name) Match.EQUAL else Match.NO_MATCH
            NameType.FQN_WILDCARD -> Match.NO_MATCH
            NameType.WILDCARD -> Match.INCLUDES
        }
        NameType.FQN_WILDCARD -> when (that.type) {
            NameType.FQN -> if (that.name.startsWith(this.name.substring(0..this.name.length-3))) Match.INCLUDES else Match.NO_MATCH
            NameType.SIMPLE -> Match.NO_MATCH
            NameType.FQN_WILDCARD -> if (this.name == that.name) Match.EQUAL else Match.NO_MATCH
            NameType.WILDCARD -> Match.NO_MATCH
        }
        NameType.WILDCARD -> when (that.type) {
            NameType.FQN -> Match.INCLUDES
            NameType.SIMPLE -> Match.INCLUDES
            NameType.FQN_WILDCARD -> Match.NO_MATCH
            NameType.WILDCARD -> Match.EQUAL
        }
    }

    /**
     * Normalizes this [Name] by removing the provided prefix. If this [Name] does not start with the given prefix, then the [Name] will not be changed.
     *
     * @param prefix The prefix [Name] relative to which the name should be normalized.
     */
    fun normalize(prefix: Name): Name = if (this.name.startsWith(prefix.name)) {
        Name(this.name.substring(prefix.name.length + 1 until this.name.length))
    } else {
        this
    }

    /**
     * Returns a [String] representation of this [Name].
     *
     * @return [String] for this [Name].
     */
    override fun toString(): String= this.name

    /**
     *
     */
    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + type.hashCode()
        return result
    }

    /**
     *
     */
    override fun equals(other: Any?): Boolean {
        if (other === this) return true
        if (other !is Name) return false
        return other.name == this.name && other.type == this.type
    }
}