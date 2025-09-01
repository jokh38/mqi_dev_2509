
## **📄 Refactoring Plan for mqi\_communicator**

### **Introduction**

This document provides the detailed technical roadmap for refactoring the `mqi_communicator` application. Its purpose is to translate the high-level objectives defined in `Refactoring_Goals.md` into a concrete, actionable plan. This plan focuses on creating a simple, robust, and scalable architecture by adopting modern software design patterns and best practices.

-----

### **1. Target Architecture: A Master/Worker Model**

To achieve the desired separation of concerns and scalability, we will implement a **Master/Worker** architecture. This model isolates the complex end-to-end workflow of a single case from the high-level task of managing multiple cases concurrently.

#### **Master Process (`main.py`)**

The Master process acts as the central orchestrator and user interface. It does not perform any of the actual case processing tasks itself. Its sole responsibilities are:

  * **Initialization:** On startup, it loads and validates the configuration, sets up logging, and initializes the database handler.
  * **Case Detection:** It uses the `watchdog` library to monitor the `new_cases` directory for incoming jobs. New jobs are added to an internal queue.
  * **Worker Pool Management:** It manages a `multiprocessing.Pool` of a configurable size (`max_workers`). This pool represents the total concurrent processing capacity of the system.
  * **Job Dispatching:** It continuously monitors the job queue and dispatches pending cases to available workers in the pool using `pool.apply_async()`.
  * **Real-time Monitoring:** It listens on a dedicated `multiprocessing.Queue` for status update messages from all worker processes. This information is used to render a real-time console dashboard using the `rich` library, providing a clear view of the entire system's status.

#### **Worker Process (`worker.py`)**

Each Worker is a short-lived, independent process responsible for handling exactly one case from start to finish. This design ensures maximum stability, as a crash in one worker will not affect the Master or any other worker. Its responsibilities are:

  * **Execution:** It is launched by the Master and receives a `case_id` as its primary argument.
  * **Workflow Execution:** It instantiates a `WorkflowManager` object, which is responsible for executing the case's multi-step workflow.
  * **Status Reporting:** After each significant step (e.g., "preprocessing complete," "uploading to HPC"), it sends a status update message `(case_id, status, message)` to the shared `multiprocessing.Queue` for the Master to display.
  * **State Persistence:** It uses its own instance of the `DatabaseHandler` to record the status of each step in the central database, ensuring the case's state is persisted.

-----

### **2. New File Structure**

The proposed structure separates concerns into distinct, single-responsibility modules.

```
mqi_communicator/
├── config/
│   └── config.yaml
├── database/
│   └── mqi_communicator.db
├── src/
│   ├── __init__.py
│   ├── main.py             # Master Process: Watches for cases, manages the process pool.
│   ├── worker.py           # Entry point for a single worker process.
│   ├── workflow_manager.py # State Pattern-based workflow context manager.
│   ├── states.py           # Contains all State classes defining each workflow step.
│   ├── local_handler.py    # Handles local CLI execution (P2, P3).
│   ├── remote_handler.py   # Handles HPC communication (SSH/SFTP).
│   ├── config.py           # Pydantic-based configuration loader and validator.
│   ├── database_handler.py # Process-safe DB interface.
│   └── logging_handler.py  # Sets up structured logging.
│   └── display_handler.py  # Manages rich console display for progress.
├── tests/
│   ├── test_workflow_states.py
│   └── test_handlers.py
...
```

-----

### **3. Core Module Design and Responsibilities**

This section details the implementation of key modules, incorporating the approved technical enhancements.

  * **`src/config.py` (Configuration with Pydantic)**

      * This module will define a series of classes inheriting from `pydantic.BaseModel` that mirror the structure of `config.yaml`.
      * Upon application startup, the YAML file will be loaded and parsed into these Pydantic models. This provides **automatic, fail-fast validation**: if any required fields are missing, if types are incorrect (e.g., `port` is not an integer), or if values are invalid, the application will exit immediately with a clear error message. This prevents configuration-related errors from occurring at runtime.

  * **`src/local_handler.py` (Robust Local Execution)**

      * All file and directory path manipulations within this module will use the **`pathlib`** library. This ensures cross-platform compatibility and improves code readability (e.g., `Path(parent_dir) / child_dir`).
      * Methods that call external programs (like `mqi_interpreter`) will use Python's `subprocess.run()`. The call will be configured with `capture_output=True`, `text=True`, and `check=False`. The method will then inspect the `returncode` and capture `stdout` and `stderr`. It will return a structured object (e.g., a dataclass) containing `success: bool`, `output: str`, and `error: str`. This provides the calling code with detailed information for logging and error handling.

  * **`src/workflow_manager.py` & `src/states.py` (State Pattern for Workflow)**

      * This is the core of the case-processing logic. The **State Design Pattern** will be used to make the workflow flexible and extensible.
      * **`states.py`** will define a `BaseState` abstract class with an `execute(context)` method. Concrete state classes like `PreProcessingState`, `FileUploadState`, `HpcExecutionState`, `DownloadState`, and `PostProcessingState` will inherit from `BaseState`.
      * Each state class is responsible for one single task. For example, `FileUploadState.execute()` will call the `RemoteHandler` to upload files. Upon successful completion, it will return an instance of the *next* state (e.g., `return HpcExecutionState()`).
      * **`workflow_manager.py`** will act as the `Context`. It holds the current state of the case. Its main loop will repeatedly call `execute()` on the current state object and update its state to whatever the method returns. This decouples the workflow sequence from the individual step implementations.

  * **`src/database_handler.py` (Process-Safe Database Interaction)**

      * To ensure process safety with `sqlite3` in a multiprocessing environment, a critical design principle will be enforced: **each worker process will instantiate its own `DatabaseHandler` object**. This ensures that each process has its own dedicated database connection, preventing conflicts over shared connection objects. The underlying SQLite database file, operating in WAL (Write-Ahead Logging) mode, is capable of handling concurrent writes from multiple processes safely.
      
  - **`src/display_handler.py`**:

      - (No change) Uses `rich` to provide a clean, real-time display.

-----

### **4. Phased Execution Plan**

The refactoring will proceed in four distinct phases.

1.  **Phase 1: Foundation and Structure**

      * Create the new file and directory structure.
      * Implement the `Pydantic` models in `src/config.py` to validate the `config.yaml` structure.
      * Set up the structured logging in `logging_handler.py`.
      * Integrate code quality tools (`Black`, `Ruff`) and static type checking (`mypy`) into the development workflow, potentially using pre-commit hooks.
      * Implement the `DatabaseHandler` class, ensuring it is designed for instantiation within each worker.

2.  **Phase 2: Handler and Workflow Implementation**

      * Implement `LocalHandler` and `RemoteHandler` using `pathlib` and the robust `subprocess` execution pattern.
      * Implement the State Pattern: define `BaseState` and all concrete state classes in `states.py`.
      * Implement the `WorkflowManager` to manage state transitions.

3.  **Phase 3: Master/Worker Integration**

      * Implement the `worker.py` entry point, which initializes and runs the `WorkflowManager` for a given case.
      * Implement the `main.py` Master process, including the `watchdog` file monitor, `multiprocessing.Pool` management, and the `multiprocessing.Queue` for status reporting.
      * Develop the `rich`-based console dashboard to display real-time status updates.

4.  **Phase 4: Quality Assurance and Cleanup**

      * Write comprehensive unit tests for each state in `states.py` and for the methods in `local_handler.py` and `remote_handler.py`.
      * Perform end-to-end integration testing with real cases.
      * Once the new system is verified and stable, safely remove all legacy files (`main_loop_logic.py`, `parallel_processor.py`, etc.).

-----

### **5. Proposed `config.yaml`**

The following configuration structure provides the necessary flexibility to manage the entire workflow from a single file.

```yaml
# config/config.yaml

# Settings for the main application controller
application:
  # Number of cases to process concurrently. Should ideally match the number of available HPC GPUs.
  max_workers: 4
  # How often (in seconds) to scan the new_cases directory.
  scan_interval_seconds: 60
  # How often (in seconds) to poll the HPC for simulation completion.
  polling_interval_seconds: 300

# Paths to the external command-line tools P1 will orchestrate
executables:
  python_interpreter: "C:\\Python310\\python.exe"
  mqi_interpreter: "C:\\MOQUI_SMC\\mqi_interpreter\\main_cli.py"
  raw_to_dicom: "C:\\MOQUI_SMC\\RawToDCM\\moqui_raw2dicom.py"

# All file system paths, both local and remote.
paths:
  local:
    # P1 will watch this directory for new case subdirectories.
    scan_directory: "C:/MOQUI_SMC/new_cases"
    # The following paths use a {case_id} placeholder that will be dynamically replaced.
    # Directory for intermediate files (CSVs) generated by P2.
    processing_directory: "C:/MOQUI_SMC/data/cases/{case_id}/intermediate"
    # Directory to download raw simulation output from HPC.
    raw_output_directory: "C:/MOQUI_SMC/data/cases/{case_id}/raw"
    # Directory for final DICOM files generated by P3.
    final_dicom_directory: "C:/MOQUI_SMC/data/cases/{case_id}/dicom"
  hpc:
    base_dir: "~/MOQUI_SMC"
    tps_env_dir: "~/MOQUI_SMC/tps_env"
    output_csv_dir: "~/MOQUI_SMC/Output_csv/{case_id}"
    dose_raw_dir: "~/MOQUI_SMC/Dose_raw/{case_id}"

# Connection details for the HPC server.
hpc_connection:
  host: "mgmt01"
  port: 22
  user: "jokh38"
  # Using SSH keys is strongly recommended over passwords for security.
  ssh_key_path: "C:/Users/your_user/.ssh/id_rsa"
```