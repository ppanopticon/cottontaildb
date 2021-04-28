package org.vitrivr.cottontail.database.queries

/** A [GroupId] is an identifier to identify sub-trees in an query execution plan. */
typealias GroupId = Int

/** A [Digest] is a hash value that uniquely identifies a tree of nodes. The same combination of nodes should lead to the same [Digest]. */
typealias Digest = Long