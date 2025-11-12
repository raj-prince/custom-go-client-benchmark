package workerpool

// Task interface defines the contract for a runnable task.
type Task interface {
	Execute()
}

type WorkerPool interface {
	// Start initializes the worker pool and prepares it to accept tasks.
	Start()

	// Stop gracefully shuts down the worker pool, waiting for all tasks to complete.
	Stop()

	// Schedule adds a task to the worker pool for execution.
	Schedule(urgent bool, task Task)
}
