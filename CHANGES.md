# Change log for Cottontail DB

## Version 0.12.7

### Bugfixes

* Refactored MigrationManager to fix several locking issues due to structuring of transactions.
* Fixed race condition in query execution due to plan cache.

## Version 0.12.6

### Bugfixes

* Fixed various bugs related to transaction management especially for DDL statements.
* Fixed an issue that reset the entity statistics during optimization
* Fixed a race condition in the LockManager

## Version 0.12.5

* There is now also a Docker container on DockerHub
* Cleaned-up some unit tests.

### Bugfixes

* Fixed a bug that prevented indexes from being closed properly when closing an entity.

## Version 0.12.4

* Cleaned-up some slight issue in legacy DBO implementation.
* Unified the pattern used to create and drop schemas, entities and indexes.

### Bugfixes

* Fixed inconsistencies in logging and error handling during query execution.
* Fixed a bug that caused locks to be released while other DBOs were still in the process of finalizing a transaction.
* Fixed a bug that caused gRPC methods to return for USER_IMPLICIT transactions before COMMIT or ROLLBACK was executed.

## Version 0.12.3

* Slight optimization as to how DefaultEntity.scan() handles sub-transactions.
* Weight vectors that contain only ones are now removed during query binding.

### Bugfixes

* Fixed the deferred fetching rule for the query planner.

## Version 0.12.2

### Bugfixes

* Fixed issue that arises if root folder or catalogue doesn't exist.

## Version 0.12.1

* Added explicit catalogue version check during start-up.

### Bugfixes

* Fixed _ping_ gRPC command (thanks to @silvanheller)
* Fixed issue in DefaultEntity.Tx.optimize() that caused double entries in Index.
* Fixed error in selectivity calculation that caused certain queries to fail.
* Fixed error in parallelization calculation that lead low-cost queries to be parallelized.

## Version 0.12.0

* <ins>Breaking:</ins> Re-organized catalogue; old instances of Cottontail DB cannot be used with version 0.12.0. Use _migration_ tool migrate an instance.
* Language: Added support for ORDER BY-clauses and sub-SELECTS.
* Language: NNS queries now only support a single query / weight vector. Batched NNS will be re-added in a future version with batched query support.
* Language: All commands except for _ping_, _begin_, _commit_, _rollback_ now return _QueryResponseMessages_
* CLI: Re-organized and extended existing CLI functionality.
* CLI: The _system_ command now hosts commands that allow for introspection into the Cottontail DB execution engine
* CLI: Added a _system migration_ command that allow for migration of old catalogue versions.
* Added transaction support; we use 2PL internally to maintain locks on database objects and enforce isolation
* Added support for incremental updates to indexes upon INSERT, UPDATE or DELETE, if an indexes supports this (#10)
* Added PQ and VA indexes for nearest neighbor search
* Added column statistics that can be used for query planning
* Re-added support for SELECT DISTINCT projectsion (#68)
* Re-structured query planner, which should now scale better to more complex query plans

### Bugfixes

* The results of entity optimization are now persistent and visible once the operation completes (#67)
