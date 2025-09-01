# In-Depth Comparison: `mqi_communicator` vs. `mqi_communicator_new`

This document provides a detailed, code-level comparison of the original `mqi_communicator` and the refactored `mqi_communicator_new`. The refactoring represents a fundamental architectural evolution from a monolithic, config-driven application to a modern, decoupled, parallel-processing system.

## 1. Architectural Comparison: A Paradigm Shift

The most significant change is the shift in the core architectural pattern.

### `mqi_communicator` (Old): Monolithic Supervisor

The original application operates as a **single, monolithic process** that acts as a supervisor.

-   **Central Main Loop:** The `main.py` contains a heavy `while True:` loop that is responsible for everything:
    -   Periodically scanning for new files.
    -   Checking the status of running jobs.
    -   Recovering from various failure states (stuck jobs, "zombie" resources).
    -   Assigning resources.
    -   Dispatching work to the `WorkflowEngine`.
-   **Config-Driven Logic:** The core workflow logic is not in the code but is defined in `config.yaml`. The `WorkflowEngine` is a complex class that parses this YAML to determine which steps to run, what commands to execute, and how to handle retries. This makes the control flow opaque and hard to debug.
-   **Limited Parallelism:** Parallel processing is bolted on via a `ParallelCaseProcessor`, but the core of the application remains a single process, creating a bottleneck and a single point of failure.

### `mqi_communicator_new` (New): Master-Worker Architecture

The refactored application adopts a modern **Master-Worker architecture** using Python's `multiprocessing` module.

-   **Lightweight Master:** The `main.py` now acts as a lightweight **Master** process (or dispatcher). Its only responsibilities are:
    -   Watching for new cases using the efficient, event-driven `watchdog` library.
    -   Managing a `multiprocessing.Pool` of worker processes.
    -   Dispatching new cases to available workers via a `Queue`.
    -   Receiving status updates from workers and updating the UI.
-   **Isolated Workers:** The actual work is performed by independent **Worker** processes (`worker.py`). Each worker:
    -   Handles a single case from start to finish.
    -   Sets up its own resources (database connections, handlers).
    -   Is completely isolated, preventing shared-state issues.
-   **Code-Driven Logic:** The workflow logic is now explicitly defined in the code using the **State Design Pattern**, which is clear, maintainable, and easy to test.

**Conclusion:** The new architecture is fundamentally more robust, scalable, and maintainable. It properly separates concerns, allowing the master to focus on dispatching while the workers focus on processing.

---

## 2. Module-by-Module Breakdown

### 2.1. Configuration (`config.yaml` & Manager)

-   **Old (`ConfigManager`)**: A simple dictionary loader. The `config.yaml` was overloaded with complex workflow definitions, including sequences of steps, commands, and retry logic. Validation was manual and minimal.
-   **New (`config.py`)**: Uses the **`pydantic` library** to define a strict schema for `config.yaml`.
    -   **Benefit**: Automatic, fail-fast validation. The application won't start with an invalid or incomplete configuration.
    -   **Benefit**: The configuration is now purely for settings (paths, credentials, worker counts), completely separating it from the workflow logic.
    -   **Benefit**: Provides a type-safe `Config` object to the application, improving developer experience and reducing bugs.

### 2.2. Workflow Logic (`WorkflowEngine` vs. State Pattern)

-   **Old (`WorkflowEngine`)**: A complex, stateful engine that reads its instructions from the YAML file. It determined the workflow at runtime by parsing strings and dictionaries. This made the logic difficult to follow and tightly coupled the engine to the config structure.
-   **New (`states.py`, `workflow_manager.py`)**: Implements the **State Design Pattern**.
    -   **`states.py`**: Each step of the workflow (e.g., `PreProcessingState`, `FileUploadState`) is a distinct class with a single `execute` method.
    -   **State Transitions**: The `execute` method of each state explicitly returns an instance of the *next* state, making the workflow transparent and easy to trace in the code.
    -   **`workflow_manager.py`**: Acts as the driver for the state machine, running a simple loop that transitions from one state to the next. It also serves as the "Context" object, providing all necessary handlers to the states.

### 2.3. Local Execution (`LocalExecutor` vs. `LocalHandler`)

-   **Old (`LocalExecutor`)**: A complex class that used `subprocess.Popen` to run local scripts. It performed real-time, line-by-line parsing of `stdout` to look for special progress strings (e.g., `PROGRESS::75`). This made the code complex and brittle.
-   **New (`local_handler.py`)**: A simplified and more robust handler.
    -   **`subprocess.run`**: It uses the simpler `subprocess.run`, which waits for the process to complete. This removes the complexity of real-time parsing.
    -   **`ExecutionResult`**: Returns a structured `NamedTuple` instead of a dictionary, improving type safety.
    -   **Responsibility**: Progress is now handled at the state level (e.g., "Preprocessing Done"), not line-by-line, which is a reasonable trade-off for a simpler, more robust design.

### 2.4. Remote Execution (`RemoteExecutor` vs. `RemoteHandler`)

This is the most dramatic technical improvement.

-   **Old (`RemoteExecutor`)**: A monolithic class that shelled out to `ssh` and `scp` command-line tools using `subprocess`.
    -   **Drawback**: Dependent on system environment, not platform-independent, and harder to test.
    -   **Drawback**: Relied on a specific HPC task manager (`pueue`), parsing its JSON output to monitor jobs. This created a tight coupling.
-   **New (`remote_handler.py`)**: A complete rewrite using the **`paramiko` library**, the standard for SSH/SFTP in Python.
    -   **Benefit**: No dependency on external executables. It's pure Python.
    -   **Benefit**: Platform-independent and far easier to test.
    -   **Benefit**: Uses SFTP for file transfers, which is more robust than `scp`.
    -   **Simplified Monitoring**: The dependency on `pueue` is removed. The new workflow simply runs the HPC job as a background process (`nohup ... &`) that creates a "marker file" (`touch done.marker`) on completion. The handler just needs to poll for the existence of this file—a much simpler and more generic approach.

## 3. Summary of Improvements

| Feature                 | `mqi_communicator` (Old)                                  | `mqi_communicator_new` (New)                                | **Advantage of New Version**                                      |
| ----------------------- | --------------------------------------------------------- | ----------------------------------------------------------- | ----------------------------------------------------------------- |
| **Architecture**        | Monolithic, single-process supervisor                     | Decoupled Master-Worker using `multiprocessing`             | Scalability, robustness, separation of concerns.                  |
| **Workflow Logic**      | Opaque, config-driven `WorkflowEngine`                    | Transparent, code-based **State Pattern**                   | Maintainability, testability, clarity.                            |
| **Configuration**       | Basic `dict` loader, logic mixed in YAML                  | **Pydantic**-based schema validation, settings-only YAML      | Fail-fast validation, type safety, improved developer experience. |
| **Remote Execution**    | `subprocess` calls to `ssh`/`scp`, coupled to `pueue`     | **`paramiko`** library, generic background process + marker file | Platform independence, testability, no external dependencies.     |
| **File Detection**      | Manual polling with `os.scandir` in a loop                | Event-driven using the **`watchdog`** library                 | Efficiency, responsiveness, modern best practice.                 |
| **Code Structure**      | Tightly coupled "services" and "managers"                 | Loosely coupled "handlers" and a clear `worker` entry point | Easier to understand, modify, and test individual components.     |
