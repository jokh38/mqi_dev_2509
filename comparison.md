# Comparison of `mqi_communicator` and `mqi_communicator_new`

This document outlines the key differences between the `mqi_communicator` and `mqi_communicator_new` projects.

## High-Level Summary

The `mqi_communicator_new` project appears to be a significant refactoring of the `mqi_communicator` project. The original project has a more complex and nested structure, while the new project is streamlined with a focus on clear separation of concerns.

Key changes include:
-   **Project Structure:** `mqi_communicator_new` has a flatter and more modular structure within the `src` directory.
-   **Configuration:** The `config.yaml` has been completely overhauled, moving from a workflow-centric configuration to a more settings-based approach.
-   **Dependencies:** The `requirements.txt` file shows changes in dependencies, with `pydantic` and `paramiko` being added in the new version, and testing-related libraries removed.
-   **Code Organization:** The code in `mqi_communicator_new` is organized into handlers (`database_handler`, `local_handler`, `remote_handler`, etc.), a `workflow_manager`, and `states`, which suggests a move towards a state machine architecture. The original project had a `services` directory with more granular responsibilities.
-   **Entry Point:** The main entry point `main.py` has moved from the root of the project into the `src` directory in `mqi_communicator_new`.

## Detailed File and Directory Differences

### Root Directory

-   **Only in `mqi_communicator`:**
    -   `CODEBASE_DOCUMENTATION.md`
    -   `Specification_FMQICOM.txt`
    -   `backups/`
    -   `docs/`
    -   `main.py`
    -   `new_cases/`
    -   `pueue_install_start.md`
    -   `pytest.ini`

### Configuration (`config/config.yaml`)

-   The configuration has been completely redesigned.
-   `mqi_communicator`: Defines a `main_workflow` as a series of steps (local and remote). It includes detailed settings for `hpc`, `scanner`, `post_processing`, `main_loop`, and `pueue`.
-   `mqi_communicator_new`: Focuses on application settings (`max_workers`, `scan_interval_seconds`), paths (local and HPC), and HPC connection details. The complex workflow logic is no longer in the config file.

### Dependencies (`requirements.txt`)

-   **Added in `mqi_communicator_new`:** `pydantic`, `paramiko`.
-   **Removed in `mqi_communicator_new`:** `pytest`, `pytest-cov`, `black`, `flake8`, `mypy`, `types-PyYAML`, `pydicom`. This suggests that the new version might have a different testing strategy or that these are considered development dependencies not required for production.

### Source Code (`src/`)

-   **`mqi_communicator` `src` structure:**
    -   `common/`: Contains shared utilities like `config_manager`, `db_manager`, `dicom_parser`, etc.
    -   `services/`: Contains the core application logic, such as `case_scanner`, `local_executor`, `remote_executor`, `workflow_engine`, etc.
    -   `dashboard.py`: A TUI dashboard.

-   **`mqi_communicator_new` `src` structure:**
    -   `config.py`: Likely for loading and validating configuration (using Pydantic).
    -   `database_handler.py`
    -   `display_handler.py`
    -   `local_handler.py`
    -   `logging_handler.py`
    -   `main.py`: The new main entry point.
    -   `remote_handler.py`
    -   `states.py`: Defines the different states of the workflow.
    -   `worker.py`: Manages the execution of tasks.
    -   `workflow_manager.py`: Orchestrates the overall workflow.

### Tests (`tests/`)

-   The test structure reflects the changes in the `src` directory.
-   `mqi_communicator`: Has tests for `common` and `services`.
-   `mqi_communicator_new`: Has tests for `handlers` and `workflow_states`.

## Conclusion

The refactoring from `mqi_communicator` to `mqi_communicator_new` is substantial. It modernizes the codebase by:
-   Adopting a more modular and maintainable architecture.
-   Separating configuration from workflow logic.
-   Using modern libraries like Pydantic for data validation.
-   Implementing what appears to be a more robust state management system for the workflow.

The new structure is likely easier to understand, test, and extend.
