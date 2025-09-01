# MQI Communicator Codebase Documentation

## 1. Overview

### Main Purpose
The MQI Communicator is a sophisticated workflow orchestration system designed to process medical physics cases using the MOQUI SMC (Scattering Monte Carlo) simulation software. It manages the complete lifecycle of case processing from detection to completion, including:

- Automated case detection and registration
- Parallel processing with priority scheduling
- Dynamic GPU resource management
- Local and remote execution of workflow steps
- Error handling and retry mechanisms
- Real-time monitoring and logging
- Database persistence for state management

### Key Features
- **Filesystem Monitoring**: Automatically detects new cases in a watched directory
- **Parallel Processing**: Processes multiple cases concurrently using ThreadPoolExecutor
- **Priority Scheduling**: Implements weighted fair queuing with aging to prevent starvation
- **Dynamic GPU Management**: Automatically discovers and manages GPU resources
- **Hybrid Execution**: Supports both local and remote (HPC) execution of workflow steps
- **Robust Error Handling**: Comprehensive error categorization and retry mechanisms
- **Real-time Dashboard**: Web-based monitoring interface
- **Structured Logging**: JSON-formatted logs with contextual information

## 2. Directory Structure

```
mqi_communicator/
├── config/
│   └── config.yaml              # Main configuration file
├── database/                    # Database storage directory
│   └── mqi_communicator.db      # SQLite database file
├── docs/                        # Documentation files
├── new_cases/                   # Directory monitored for new cases
├── src/                         # Source code
│   ├── __init__.py
│   ├── common/                  # Shared utilities and components
│   │   ├── __init__.py
│   │   ├── config_manager.py    # Configuration loading and validation
│   │   ├── db_manager.py        # Database management with optimization
│   │   ├── dicom_parser.py      # DICOM file parsing utilities
│   │   ├── error_categorization.py  # Error classification system
│   │   ├── retry_policy.py      # Retry logic for failed operations
│   │   ├── rich_display.py      # Progress display utilities
│   │   └── structured_logging.py  # Structured JSON logging
│   ├── services/                # Core service implementations
│   │   ├── __init__.py
│   │   ├── case_scanner.py      # Filesystem monitoring for new cases
│   │   ├── dynamic_gpu_manager.py  # GPU resource discovery and management
│   │   ├── local_executor.py    # Local command execution
│   │   ├── main_loop_logic.py   # Core application loop logic
│   │   ├── parallel_processor.py  # Parallel case processing
│   │   ├── priority_scheduler.py  # Priority-based case scheduling
│   │   ├── remote_executor.py   # Remote (HPC) command execution
│   │   ├── tps_generator.py     # MOQUI TPS configuration generation
│   │   └── workflow_engine.py   # Workflow orchestration engine
│   └── dashboard.py             # Web-based monitoring dashboard
├── tests/                       # Unit and integration tests
├── main.py                      # Application entry point
├── requirements.txt             # Python dependencies
└── ...
```

## 3. Module Calling Structure

```mermaid
graph TD
    A[main.py] --> B[ConfigManager]
    A --> C[DatabaseManager]
    A --> D[CaseScanner]
    A --> E[WorkflowEngine]
    A --> F[DynamicGpuManager]
    A --> G[PriorityScheduler]
    A --> H[ParallelCaseProcessor]
    A --> I[Dashboard]
    
    E --> J[LocalExecutor]
    E --> K[RemoteExecutor]
    E --> L[TPSGenerator]
    
    H --> E
    H --> F
    H --> G
    
    D --> C
    E --> C
    F --> C
    G --> C
    H --> C
    
    J --> B
    K --> B
    L --> B
    
    subgraph "Core Components"
        B
        C
        D
        E
        F
        G
        H
        I
        J
        K
        L
    endgraph
    
    subgraph "Utilities"
        M[StructuredLogging]
        N[ErrorCategorization]
        O[RichDisplay]
        P[DicomParser]
        Q[RetryPolicy]
    endgraph
    
    A --> M
    C --> M
    D --> M
    E --> M
    F --> M
    G --> M
    H --> M
    I --> M
    J --> M
    K --> M
    L --> M
    
    E --> N
    J --> N
    K --> N
    
    E --> O
    J --> O
    K --> O
    
    L --> P
    L --> Q
```

## 4. Core Components Documentation

### 4.1 Main Application (main.py)

**Purpose**: Entry point and orchestrator for the entire application.

**Key Variables**:
- `CONFIG_PATH`: Path to configuration file
- `KST`: Korea Standard Time timezone object

**Key Functions**:
- `setup_logging(config)`: Initializes structured logging system
- `main(config)`: Main application loop that initializes all components and runs the processing loop

**Dependencies**:
- `src.common.db_manager.DatabaseManager`
- `src.common.config_manager.ConfigManager`
- `src.common.structured_logging`
- `src.services.case_scanner.CaseScanner`
- `src.services.workflow_engine.WorkflowEngine`
- `src.services.dynamic_gpu_manager.DynamicGpuManager`
- `src.services.priority_scheduler.PriorityScheduler`
- `src.services.parallel_processor.ParallelCaseProcessor`
- `src.services.main_loop_logic`
- `src.dashboard`

### 4.2 Configuration Management (src/common/config_manager.py)

**Purpose**: Loads, validates, and provides access to application configuration.

**Key Classes**:
- `ConfigValidationError`: Exception for configuration validation failures
- `ConfigManager`: Main configuration management class

**Key Variables**:
- `SCHEMA`: Configuration schema with required fields and types

**Key Functions**:
- `_load_and_validate_config()`: Loads and validates configuration from file
- `_apply_defaults_and_validate(config)`: Applies defaults and validates against schema
- `get(key, default)`: Gets configuration value using dot notation
- `get_section(section_name)`: Gets entire configuration section
- `reload()`: Reloads configuration from file

### 4.3 Database Management (src/common/db_manager.py)

**Purpose**: Manages SQLite database with performance optimizations including caching, indexing, and connection pooling.

**Key Classes**:
- `QueryPerformanceMetrics`: Tracks database query performance
- `QueryCache`: LRU cache for database query results
- `DatabaseManager`: Main database management class

**Key Variables**:
- `KST`: Korea Standard Time timezone
- `enable_cache`: Whether to enable query caching
- `enable_wal_mode`: Whether to enable WAL mode for better concurrency

**Key Functions**:
- `_create_optimized_connection()`: Creates optimized SQLite connection
- `_execute_with_metrics(query, params, cache_key)`: Executes query with performance tracking
- `_create_tables()`: Creates necessary database tables
- `_create_indexes()`: Creates performance indexes
- `init_db()`: Initializes database with tables and indexes
- `add_case(case_path, priority)`: Adds new case to database
- `get_case_by_id(case_id)`: Retrieves case by ID
- `get_cases_by_status(status, limit)`: Gets cases by status
- `update_case_status(case_id, status, progress)`: Updates case status
- `find_and_lock_any_available_gpu(case_id)`: Atomically finds and locks available GPU

### 4.4 Case Scanner (src/services/case_scanner.py)

**Purpose**: Monitors filesystem for new cases and registers them in the database.

**Key Classes**:
- `_NewCaseHandler`: Internal handler for filesystem events
- `CaseScanner`: Main case scanning class

**Key Variables**:
- `watch_path`: Directory to monitor for new cases
- `quiescence_period`: Time to wait for file copy completion

**Key Functions**:
- `_add_case_if_not_exists(case_path)`: Adds case to database if not already present
- `perform_initial_scan()`: Scans watch path for pre-existing directories
- `start()`: Starts filesystem observer
- `stop()`: Stops filesystem observer

### 4.5 Workflow Engine (src/services/workflow_engine.py)

**Purpose**: Orchestrates the complete lifecycle of case processing through configurable workflows.

**Key Classes**:
- `WorkflowExecutionError`: Custom exception for workflow execution errors
- `WorkflowEngine`: Main workflow orchestration engine

**Key Variables**:
- `config`: Application configuration
- `main_workflow`: Configured workflow steps
- `local_executor`: Local command executor
- `remote_executor`: Remote command executor

**Key Functions**:
- `process_case(case_id, case_path, pueue_group)`: Processes case through complete workflow
- `_determine_starting_step(current_status)`: Determines workflow starting point
- `_execute_workflow_step(step_config, case_id, case_path, pueue_group, display)`: Executes single workflow step
- `find_task_by_label(label)`: Finds pueue task by label
- `kill_workflow(task_id)`: Kills running workflow
- `get_workflow_status(task_id)`: Gets workflow status

### 4.6 Dynamic GPU Manager (src/services/dynamic_gpu_manager.py)

**Purpose**: Discovers and manages GPU resources dynamically.

**Key Classes**:
- `DynamicGpuManager`: Main GPU resource management class

**Key Variables**:
- `config`: Application configuration
- `db_manager`: Database manager instance

**Key Functions**:
- `refresh_gpu_resources()`: Discovers and updates GPU resources
- `get_optimal_gpu_assignment()`: Gets optimal GPU for case assignment

### 4.7 Priority Scheduler (src/services/priority_scheduler.py)

**Purpose**: Implements priority-based case scheduling algorithms.

**Key Classes**:
- `PriorityConfig`: Configuration for priority scheduling
- `PriorityScheduler`: Main priority scheduling class

**Key Variables**:
- `config`: Priority scheduling configuration
- `db_manager`: Database manager instance

**Key Functions**:
- `get_prioritized_cases(status, limit)`: Gets cases ordered by priority
- `calculate_priority(case)`: Calculates case priority based on algorithm

### 4.8 Parallel Processor (src/services/parallel_processor.py)

**Purpose**: Handles parallel processing of multiple cases with optimal resource utilization.

**Key Classes**:
- `ProcessingMetrics`: Tracks parallel processing performance
- `ParallelCaseProcessor`: Main parallel processing class

**Key Variables**:
- `max_workers`: Maximum concurrent processing threads
- `batch_size`: Maximum cases to process in one batch
- `processing_timeout`: Timeout for individual case processing

**Key Functions**:
- `process_case_batch()`: Processes batch of cases in parallel
- `_process_single_case(case)`: Processes single case
- `_assign_optimal_gpu(case_id)`: Assigns optimal GPU to case
- `get_performance_summary()`: Gets performance metrics

### 4.9 Local Executor (src/services/local_executor.py)

**Purpose**: Executes commands locally on the system.

**Key Classes**:
- `LocalExecutionError`: Exception for local execution errors
- `LocalExecutor`: Main local execution class

**Key Variables**:
- `config`: Application configuration

**Key Functions**:
- `execute(target, context, display)`: Executes local command
- `_run_interpreter(context, display)`: Runs MOQUI interpreter
- `_generate_tps_config(context, display)`: Generates TPS configuration
- `_run_raw2dcm(context, display)`: Converts raw output to DICOM

### 4.10 Remote Executor (src/services/remote_executor.py)

**Purpose**: Executes commands remotely on HPC systems.

**Key Classes**:
- `RemoteExecutionError`: Exception for remote execution errors
- `RemoteExecutor`: Main remote execution class

**Key Variables**:
- `config`: Application configuration
- `hpc_config`: HPC-specific configuration

**Key Functions**:
- `execute(target, context, display)`: Executes remote command
- `_execute_commands(context, display)`: Executes remote commands
- `_upload_files(context, display)`: Uploads files to remote system
- `_download_files(context, display)`: Downloads files from remote system
- `get_workflow_status(task_id)`: Gets remote workflow status

### 4.11 TPS Generator (src/services/tps_generator.py)

**Purpose**: Generates MOQUI TPS configuration files.

**Key Functions**:
- `create_ini_content(config_params)`: Creates INI file content
- `validate_required_parameters(config_params, required_params)`: Validates required parameters
- `generate_tps_config(case_path, config)`: Generates complete TPS configuration

### 4.12 Main Loop Logic (src/services/main_loop_logic.py)

**Purpose**: Implements core application loop logic.

**Key Functions**:
- `recover_stuck_submitting_cases(db_manager, workflow_engine)`: Recovers stuck cases
- `manage_running_cases(db_manager, workflow_engine, timeout_delta, kst)`: Manages running cases
- `manage_zombie_resources(db_manager, workflow_engine)`: Manages zombie resources
- `process_new_submitted_cases_parallel(db_manager, workflow_engine, parallel_processor)`: Processes cases in parallel
- `process_new_submitted_cases_with_optimization(db_manager, workflow_engine, gpu_manager)`: Processes cases with optimization

### 4.13 Dashboard (src/dashboard.py)

**Purpose**: Provides web-based monitoring interface.

**Key Variables**:
- `app`: Flask application instance
- `db_manager`: Database manager instance

**Key Functions**:
- `get_cases()`: Gets cases for display
- `get_gpu_resources()`: Gets GPU resources for display
- `index()`: Main dashboard route

### 4.14 Structured Logging (src/common/structured_logging.py)

**Purpose**: Provides structured JSON logging with contextual information.

**Key Classes**:
- `LogContext`: Contextual logging information
- `JsonFormatter`: JSON formatter for log records

**Key Functions**:
- `get_structured_logger(name, initial_context)`: Gets structured logger
- `format(record)`: Formats log record as JSON

### 4.15 Error Categorization (src/common/error_categorization.py)

**Purpose**: Categorizes errors for appropriate handling.

**Key Classes**:
- `BaseExecutionError`: Base execution error class

**Key Functions**:
- `categorize_error(error, context)`: Categorizes error based on type and context

### 4.16 Rich Display (src/common/rich_display.py)

**Purpose**: Provides rich console display for progress tracking.

**Key Classes**:
- `ProgressDisplay`: Main progress display class

**Key Functions**:
- `create_progress_display(case_name, case_id)`: Creates progress display

### 4.17 DICOM Parser (src/common/dicom_parser.py)

**Purpose**: Parses DICOM files for workflow processing.

**Key Functions**:
- `find_rtplan_file(dicom_dir)`: Finds RTPLAN file in directory
- `get_plan_info(rtplan_path)`: Gets plan information from RTPLAN file

### 4.18 Retry Policy (src/common/retry_policy.py)

**Purpose**: Implements retry logic for failed operations.

**Key Functions**:
- Various retry policy implementations

## 5. Configuration Structure

The application configuration is defined in `config/config.yaml` and is loaded by `ConfigManager`.

### 5.1 Logging Configuration
```yaml
logging:
  path: "communicator_local.log"
```

### 5.2 Database Configuration
```yaml
database:
  path: "database/mqi_communicator.db"
```

### 5.3 Dashboard Configuration
```yaml
dashboard:
  auto_start: true
```

### 5.4 Main Workflow Configuration
```yaml
main_workflow:
  - name: "Generate Moqui Config"
    type: local
    target: generate_tps_config
    # ... additional workflow step configuration
```

### 5.5 HPC Configuration
```yaml
hpc:
  host: "10.243.62.128"
  user: "jokh38"
  # ... additional HPC configuration
```

### 5.6 Scanner Configuration
```yaml
scanner:
  watch_path: "new_cases"
  quiescence_period_seconds: 5
```

### 5.7 Main Loop Configuration
```yaml
main_loop:
  sleep_interval_seconds: 10
  running_case_timeout_hours: 24
  parallel_processing:
    enabled: true
    max_workers: 4
    # ... additional parallel processing configuration
  priority_scheduling:
    enabled: true
    algorithm: "weighted_fair"
    # ... additional priority scheduling configuration
```

### 5.8 Pueue Configuration
```yaml
pueue:
  groups:
    - "gpu_0"
    - "gpu_1"
    # ... additional pueue groups
```

### 5.9 MOQUI TPS Parameters
```yaml
moqui_tps_parameters:
  GPUID: 0
  RandomSeed: -1932780356
  # ... additional MOQUI TPS parameters
```