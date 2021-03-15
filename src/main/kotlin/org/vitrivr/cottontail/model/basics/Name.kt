package org.vitrivr.cottontail.model.basics

/**
 * A [Name] that identifies a DBO used within Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class Name {

    companion object {
        /* Delimiter between name components. */
        const val NAME_COMPONENT_DELIMITER = "."

        /* Character used for wildcards. */
        const val NAME_COMPONENT_WILDCARD = "*"

        /* Root name component in Cottontail DB names. */
        const val NAME_COMPONENT_ROOT = "warren"
    }

    /** Internal array of [Name] components. */
    abstract val components: Array<String>

    /** Returns the last component of this [Name], i.e. the simple name. */
    open val simple: String
        get() = this.components.last()

    /** Returns true if this [Name] matches the other [Name]. */
    abstract fun matches(other: Name): Boolean

    /**
     * The [RootName] which always is 'warren'.
     */
    object RootName : Name() {
        override val components: Array<String> = arrayOf(NAME_COMPONENT_ROOT)
        override fun matches(other: Name): Boolean = (other == this)
        fun schema(name: String) = SchemaName(NAME_COMPONENT_ROOT, name)
    }

    /**
     * A [Name] object used to identify a [Schema][org.vitrivr.cottontail.database.schema.DefaultSchema].
     */
    class SchemaName(vararg components: String) : Name() {

        /** Normalized [Name] components of this [SchemaName]. */
        override val components = when {
            components.size == 1 -> arrayOf(
                NAME_COMPONENT_ROOT,
                components[0].toLowerCase()
            )
            components.size == 2 && components[0].toLowerCase() == NAME_COMPONENT_ROOT -> arrayOf(
                NAME_COMPONENT_ROOT,
                components[1].toLowerCase()
            )
            else -> throw IllegalStateException("${components.joinToString(".")} is not a valid schema name.")
        }

        /**
         * Returns [RootName] of this [SchemaName].

         * @return [RootName]
         */
        fun root() = RootName

        /**
         * Generates an [EntityName] as child of this [SchemaName].
         *
         * @param name Name of the [EntityName]
         * @return [EntityName]
         */
        fun entity(name: String) = EntityName(*this.components, name)

        /**
         * Checks for a match with another [Name]. Only exact matches, i.e. equality, are possible for [SchemaName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)
    }

    /**
     * A [Name] object used to identify a [Entity][org.vitrivr.cottontail.database.entity.DefaultEntity].
     */
    class EntityName(vararg components: String) : Name() {

        /** Normalized [Name] components of this [EntityName]. */
        override val components = when {
            components.size == 2 -> arrayOf(
                NAME_COMPONENT_ROOT,
                components[0].toLowerCase(),
                components[1].toLowerCase()
            )
            components.size == 3 && components[0].toLowerCase() == NAME_COMPONENT_ROOT -> arrayOf(
                components[0].toLowerCase(),
                components[1].toLowerCase(),
                components[2].toLowerCase()
            )
            else -> throw IllegalStateException("${components.joinToString(".")} is not a valid entity name.")
        }

        /**
         * Returns [RootName] of this [EntityName].
         *
         * @return [RootName]
         */
        fun root() = RootName

        /**
         * Returns parent [SchemaName] for this [EntityName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName = SchemaName(*this.components.copyOfRange(0, 2))

        /**
         * Generates an [IndexName] as child of this [EntityName].
         *
         * @param name Name of the [IndexName]
         * @return [IndexName]
         */
        fun index(name: String) = IndexName(*this.components, name)

        /**
         * Generates an [ColumnName] as child of this [EntityName].
         *
         * @param name Name of the [ColumnName]
         * @return [ColumnName]
         */
        fun column(name: String) = ColumnName(*this.components, name)

        /**
         * Checks for a match with another name. Only exact matches, i.e. equality, are possible for [EntityName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)
    }

    /**
     * A [Name] object used to identify a [Index][org.vitrivr.cottontail.database.index.AbstractIndex].
     */
    class IndexName(vararg components: String) : Name() {

        /** Normalized [Name] components of this [IndexName]. */
        override val components = when {
            components.size == 3 -> arrayOf(
                NAME_COMPONENT_ROOT,
                components[0].toLowerCase(),
                components[1].toLowerCase(),
                components[2].toLowerCase()
            )
            components.size == 4 && components[0].toLowerCase() == NAME_COMPONENT_ROOT -> arrayOf(
                components[0].toLowerCase(),
                components[1].toLowerCase(),
                components[2].toLowerCase(),
                components[3].toLowerCase()
            )
            else -> throw IllegalStateException("${components.joinToString(".")} is not a valid index name.")
        }

        /**
         * Returns [RootName] of this [EntityName].
         *
         * @return [RootName]
         */
        fun root() = RootName

        /**
         * Returns parent [SchemaName] of this [IndexName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName = SchemaName(*this.components.copyOfRange(0, 2))

        /**
         * Returns parent  [EntityName] of this [IndexName].
         *
         * @return Parent [EntityName]
         */
        fun entity(): EntityName = EntityName(*this.components.copyOfRange(0, 3))

        /**
         * Checks for a match with another name. Only exact matches, i.e. equality, are possible for [IndexName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean = (other == this)
    }

    /**
     * A [Name] object used to identify a [Index][org.vitrivr.cottontail.database.column.Column].
     */
    class ColumnName(vararg components: String) : Name() {

        /** Normalized [Name] components of this [IndexName]. */
        override val components: Array<String> = when {
            components.size == 1 -> arrayOf(
                NAME_COMPONENT_ROOT,
                "*",
                "*",
                components[0].toLowerCase()
            )
            components.size == 2 -> arrayOf(
                NAME_COMPONENT_ROOT,
                "*",
                components[0].toLowerCase(),
                components[1].toLowerCase()
            )
            components.size == 3 -> arrayOf(
                NAME_COMPONENT_ROOT,
                components[0].toLowerCase(),
                components[1].toLowerCase(),
                components[2].toLowerCase()
            )
            components.size == 4 && components[0].toLowerCase() == NAME_COMPONENT_ROOT -> arrayOf(
                components[0].toLowerCase(),
                components[1].toLowerCase(),
                components[2].toLowerCase(),
                components[3].toLowerCase()
            )
            else -> throw IllegalStateException("${components.joinToString(".")} is not a valid column name.")
        }

        /** True if this [ColumnName] is a wildcard. */
        val wildcard: Boolean = this.components.any { it == NAME_COMPONENT_WILDCARD }

        /**
         * Returns [RootName] of this [EntityName].
         *
         * @return [RootName]
         */
        fun root() = RootName

        /**
         * Returns parent [SchemaName] of this [ColumnName].
         *
         * @return Parent [SchemaName]
         */
        fun schema(): SchemaName? = if (this.components[1] != NAME_COMPONENT_WILDCARD) {
            SchemaName(NAME_COMPONENT_ROOT, this.components[1])
        } else {
            null
        }

        /**
         * Returns parent [EntityName] of this [ColumnName].
         *
         * @return Parent [EntityName]
         */
        fun entity(): EntityName? = if (this.components[2] != NAME_COMPONENT_WILDCARD) {
            EntityName(NAME_COMPONENT_ROOT, this.components[1], this.components[2])
        } else {
            null
        }

        /**
         * Checks for a match with another name. Wildcard matches  are possible for [ColumnName]s.
         *
         * @param other [Name] to compare to.
         * @return True on match, false otherwise.
         */
        override fun matches(other: Name): Boolean {
            if (other !is ColumnName) return false
            if (!this.wildcard) return (this == other)
            for ((i, c) in this.components.withIndex()) {
                if (c != NAME_COMPONENT_WILDCARD && c != other.components[i]) {
                    return false
                }
            }
            return true
        }

        /**
         * Transforms this [Name] to a [String]
         *
         * @return [String] representation of this [Name].
         */
        override fun toString(): String =
            if (this.components[1] == NAME_COMPONENT_WILDCARD && this.components[2] == NAME_COMPONENT_WILDCARD) {
                components[3]
            } else {
                super.toString()
            }
    }

    /**
     * Compares this [Name] with any other [Any] and returns true, if the two are equal and false otherwise.
     *
     * @param other The [Any] to compare to.
     * @return True on equality, false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (other !is Name) return false
        if (other.javaClass != this.javaClass) return false
        if (!other.components.contentEquals(this.components)) return false
        return true
    }

    /**
     * Custom hash codes for [Name] objects.
     *
     * @return Hash code for this [Name] object
     */
    override fun hashCode(): Int {
        return 42 + this.components.contentHashCode()
    }

    /**
     * Transforms this [Name] to a [String]
     *
     * @return [String] representation of this [Name].
     */
    override fun toString(): String = this.components.joinToString(NAME_COMPONENT_DELIMITER)
}