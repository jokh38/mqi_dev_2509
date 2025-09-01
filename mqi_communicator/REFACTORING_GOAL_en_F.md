
## **📄 Refactoring Goals for mqi\_communicator**

### **1. Analysis of the Current State**

The `mqi_communicator` (P1) is a complex orchestration system designed to automate the MOQUI simulation workflow. The existing codebase reveals a sophisticated architecture featuring components for parallel processing (`ParallelCaseProcessor`), dynamic GPU management (`DynamicGpuManager`), and priority scheduling (`PriorityScheduler`). While powerful, this architecture is significantly more complex than what is required for the essential, day-to-day workflow.

The core required process, as identified from user feedback and operational analysis, is a straightforward, linear sequence for each individual case:

1.  **Local Detection:** A new case is detected in a local directory.
2.  **Local Interpretation:** The `mqi_interpreter` (P2) is called to generate intermediate CSV files.
3.  **Local Generation:** A TPS (Treatment Planning System) configuration file is generated.
4.  **Remote Transfer (Upload):** The necessary files (CSVs, TPS config) are transferred to an HPC cluster.
5.  **Remote Execution:** The MOQUI simulation is executed on the HPC.
6.  **Remote Transfer (Download):** The resulting `.raw` dose files are transferred back to the local machine.
7.  **Local Conversion:** `RawToDCM` (P3) is called to convert the `.raw` files into DICOM format.

The current implementation, with its many interconnected modules orchestrated by `main_loop_logic`, is not aligned with this simple, per-case workflow. This discrepancy is the primary driver for this refactoring initiative.

-----

### **2. Identified Problems with the Current Architecture**

The mismatch between the current architecture and the required workflow leads to several critical issues that hinder development, maintenance, and operational stability.

  * **Over-engineering:** The system includes features like local GPU management and complex priority scheduling, which introduce significant overhead for a workflow that can be handled sequentially for each incoming case. These features add layers of complexity that are rarely, if ever, utilized, making the system unnecessarily difficult to manage.
  * **Tight Coupling and "Spaghetti Code":** Responsibilities are spread across numerous modules with strong dependencies, making it difficult to trace the execution flow for a single case. This tight coupling means that a change in one part of the system can have unforeseen consequences in another, increasing the risk of introducing bugs.
  * **Lack of Clarity:** The current structure obscures the simple, linear nature of the required workflow. It is difficult for a new developer to understand the end-to-end process for a single case without delving into multiple, complex, and interrelated modules.
  * **Obscure State Management:** While a database is used for state tracking, its integration with the complex application logic makes monitoring the precise status of a specific case difficult and unintuitive. It is not immediately clear where a case is in the workflow or why it might have failed.

-----

### **3. Core Refactoring Objectives**

The primary goal is to refactor `mqi_communicator` to directly implement the required local-remote-local workflow in a clear and maintainable way. This involves eliminating unnecessary components while retaining and simplifying essential features like database state management, logging, and configuration. The key objectives are:

  * **Clarity and Simplicity:** Isolate the logic for processing a single case into a dedicated, easy-to-understand workflow manager. This will make the codebase more approachable, easier to debug, and simpler to maintain.
  * **Scalability:** Design a system that can handle a growing number of cases by increasing the number of concurrent worker processes without requiring architectural changes. The system's throughput should scale linearly with the allocated resources.
  * **Robustness and Stability:** Isolate each case's execution in its own process to prevent a single case failure from crashing the entire application. The system must be resilient to transient issues (like network interruptions) and provide clear, actionable feedback for non-recoverable errors.
  * **Testability:** Decouple the core components (local processing, remote execution, database interaction) to make them independently testable. This will allow for the creation of a comprehensive suite of unit tests, improving code quality and reliability.
  * **Maintainability:** By achieving the goals above, the overall effort required to fix bugs, add new features, and manage the system will be significantly reduced.

-----

### **4. Target Workflow and Program Roles**

The refactored system will be orchestrated by P1 (`mqi_communicator`), which acts as the central coordinator for all steps, clarifying the separation of concerns between the local machine and the HPC server.

#### **Target Workflow Diagram**

```
[Local PC]                                [HPC Server]
1. Detect new case (DICOM files, etc.)
   (P1 watches a directory)
    |
    v
2. Call P2 (mqi_interpreter) to generate
   Moqui input files (CSVs, moqui_tps.in)
    |
    v
3. Transfer Moqui input files via SFTP ------------------> (P1 sends files)
    |
    v
4. Command HPC to execute Moqui via SSH
   (P1 issues remote command)
                                                 |
                                                 v
                                             5. Perform Moqui calculation
                                                 |
                                                 v
                                             6. Generate Raw result files
    |
    v
7. Download Raw result files via SFTP <---------------------- (P1 retrieves files)
    |
    v
8. Call P3 (RawToDCM) to convert
   Raw files to DICOM format
    |
    v
9. Finalize process and log status
```

#### **Roles & Responsibilities (R\&R)**

  * **P1 (`mqi_communicator`): The Orchestrator**

      * Continuously scans the `new_cases` folder to detect new jobs.
      * When a new job is detected, it invokes P2 to perform preprocessing.
      * Connects to the HPC server via SSH/SFTP to upload necessary files and execute the simulation command.
      * Polls the HPC to check for calculation completion.
      * Downloads the result files to the local PC upon completion.
      * Invokes P3 to perform post-processing (DICOM conversion).
      * Records the status, progress, and outcome of all processes in its internal database (`mqi_communicator.db`).

  * **P2 (`mqi_interpreter`): The Preprocessor**

      * A command-line tool called by P1.
      * It analyzes the input DICOM files for a specific case.
      * It generates the input files (`moqui_tps.in`) and related CSV data (`LS_doserate.csv`, etc.) required for the Moqui simulation.

  * **P3 (`RawToDCM`): The Postprocessor**

      * A command-line tool called by P1.
      * It converts the `dose.raw` file, the output from the HPC simulation, into the standard DICOM format for clinical use.