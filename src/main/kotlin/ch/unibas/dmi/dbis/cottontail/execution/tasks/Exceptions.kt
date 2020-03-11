package ch.unibas.dmi.dbis.cottontail.execution.tasks

import ch.unibas.dmi.dbis.cottontail.execution.ExecutionPlan
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask

/**
 * This exceptions is thrown whenever a single [ExecutionPlan] fails.
 *
 * @param plan The [ExecutionPlan] that failed.
 * @param message An error message describing the circumstances.
 */
class ExecutionPlanException(plan: ExecutionPlan, message: String) : Throwable("Execution failed for execution plan ${plan.id}: $message.")

/**
 * This exceptions is thrown whenever the setup of an [ExecutionPlan] fails. Errors of this kind are is usually caused
 * by mal-specification.
 *
 * @param plan The [ExecutionPlan] that failed.
 * @param message An error message describing the circumstances.
 */
class ExecutionPlanSetupException(plan: ExecutionPlan, message: String) : Throwable("Setup failed for execution plan ${plan.id}: $message.")

/**
 * This exceptions is thrown whenever a single [ExecutionTask] fails.
 *
 * @param task The [ExecutionTask] that failed.
 * @param message An error message describing the circumstances.
 */
class TaskExecutionException(task: ExecutionTask, message: String) : Throwable("Execution failed for task ${task.id}: $message.")

/**
 * This exceptions is thrown whenever the setup of a single [ExecutionTask] fails. Errors of this kind are is usually caused
 * by mal-specification.
 *
 * @param task The [ExecutionTask] that failed.
 * @param message An error message describing the circumstances.
 */
class TaskSetupException(task: ExecutionTask, message: String) : Throwable("Setup failed for task ${task.id}: $message.")