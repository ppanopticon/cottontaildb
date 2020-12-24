package org.vitrivr.cottontail.model.basics

/**
 * A [Name] that identifies a DBO used within Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
sealed class Name(vararg components: String) {

    companion object {
        /* Default delimiter between name components. */
        const val NAME_COMPONENT_DELIMITER = "."

        /* Default delimiter between name components. */
        const val NAME_COMPONENT_ROOT = "warren"
    }

    /** Internal array of [Name] components. */
    open val components = if (components[0] == NAME_COMPONENT_ROOT) {
        components.map { it.toLowerCase() }.toTypedArray()
    } else {
        arrayOf(NAME_COMPONENT_ROOT, *components.map { it.toLowerCase() }.toTypedArray())
    }

    /** Returns the last component of this [Name], i.e. the simple name. */
    open val simple: String
        get() = this.components.last()

    /** Returns true if this [Name] matches the other [Name]. */
    abstract fun matches(other: Name): Boolean

    /**
     * The [RootName] which always is 'warren'.
     */
    object RootName : Name(NAME_COMPONENT_ROOT) {
        override fun matches(other: Name): Boolean = (other == this)
    }

    /**
     * A [Name] object used to identify a [Schema][org.vitrivr.cottontail.database.schema.Schema].
     */
    class SchemaName(vararg components: String) : Name(*components) {
        init {
            if (components[0] == NAME_COMPONENT_ROOT) {
                require(components.size == 2) { "$this is not a valid schema name." }
            } else {
                require(components.size == 1) { "$this is not a valid schema name." }
            }
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

        override fun matches(other: Name): Boolean = (other == this)
    }

    /**
     * A [Name] object used to identify a [Entity][org.vitrivr.cottontail.database.entity.Entity].
     */
    class EntityName(vararg components: String) : Name(*components) {
        init {
            if (components[0] == NAME_COMPONENT_ROOT) {
                require(components.size == 3) { "$this is not a valid entity name." }
            } else {
                require(components.size == 2) { "$this is not a valid entity name." }
            }
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


        override fun matches(other: Name): Boolean = (other == this)
    }

    /**
     * A [Name] object used to identify a [Index][org.vitrivr.cottontail.database.index.Index].
     */
    class IndexName(vararg components: String) : Name(*components) {
        init {
            if (components[0] == NAME_COMPONENT_ROOT) {
                require(components.size == 4) { "$this is not a valid index name." }
            } else {
                require(components.size == 3) { "$this is not a valid index name." }
            }
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

        override fun matches(other: Name): Boolean = (other == this)
    }

    /**
     * A [Name] object used to identify a [Index][org.vitrivr.cottontail.database.column.Column].
     */
    class ColumnName(vararg components: String) : Name(*components) {
        init {
            if (components[0] == NAME_COMPONENT_ROOT) {
                require(components.size == 4) { "$this is not a valid column name." }
            } else {
                require(components.size == 3 || components.size == 1) { "$this is not a valid column name." }
            }
        }

        override val components: Array<String> = when (components.size) {
            3 -> arrayOf(NAME_COMPONENT_ROOT, *components.map { it.toLowerCase() }.toTypedArray())
            else -> components.map { it.toLowerCase() }.toTypedArray()
        }

        /** True if this [ColumnName] is a wildcard. */
        val wildcard: Boolean
            get() = this.components.last() == "*"

        /** True if this [ColumnName] is a fully qualified name. */
        val fqn: Boolean
            get() = this.components.size == 4

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
        fun schema(): SchemaName? = if (this.components.size == 4) {
            SchemaName(*this.components.copyOfRange(0, 2))
        } else {
            null
        }

        /**
         * Returns parent [EntityName] of this [ColumnName].
         *
         * @return Parent [EntityName]
         */
        fun entity(): EntityName? = if (this.components.size == 4) {
            EntityName(*this.components.copyOfRange(0, 3))
        } else {
            null
        }

        /**
         *
         */
        override fun matches(other: Name): Boolean = if (this.wildcard) {
            if (other is ColumnName) {
                this.schema() == other.schema()
            } else {
                false
            }
        } else {
            other == this
        }
    }

    /**
     * Compares this [Name] with any other [Any] and returns true, if the two are equal and false otherwise.
     *
     * @param other The [Any] to compare to.
     * @return True on equality, false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (other !is Name) {
            return false
        }
        if (other.javaClass != this.javaClass) {
            return false
        }

        if (!other.components.contentEquals(this.components)) {
            return false
        }

        return true
    }

    /**
     * Custom hashcodes for [Name] objects.
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