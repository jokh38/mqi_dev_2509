import pytest
from unittest.mock import patch, call
import logging
from datetime import datetime, timezone

from main import main


@pytest.fixture(autouse=True)
def mute_logging():
    """Fixture to mute logging output during tests."""
    logging.disable(logging.CRITICAL)
    yield
    logging.disable(logging.NOTSET)


@pytest.fixture
def mock_config():
    """Provides a default mock config for tests, reflecting the new structure."""
    return {
        "logging": {"path": "test.log"},
        "database": {"path": "test.db"},
        "scanner": {"watch_path": "test_cases"},
        "pueue": {
            "groups": ["gpu_a", "gpu_b"]
        },  # Use 'groups' instead of 'default_group'
        "hpc": {
            "host": "test_host",
            "user": "test_user",
            "remote_base_dir": "/remote/test",
        },
        "main_loop": {"sleep_interval_seconds": 0.01},  # Use a very short sleep
    }


# --- Mocks Fixture ---
@pytest.fixture
def mock_dependencies(mock_config):
    """A single fixture to manage all patched dependencies."""
    with patch("main.time.sleep") as mock_sleep, patch(
        "main.WorkflowEngine"
    ) as MockWorkflowEngine, patch("main.CaseScanner") as MockCaseScanner, patch(
        "main.DatabaseManager"
    ) as MockDatabaseManager, patch(
        "main.ConfigManager"
    ) as MockConfigManager:

        # Configure ConfigManager mock
        mock_config_manager_instance = MockConfigManager.return_value
        mock_config_manager_instance.config = mock_config

        # Make mocks accessible
        mocks = {
            "sleep": mock_sleep,
            "WorkflowEngine": MockWorkflowEngine,
            "CaseScanner": MockCaseScanner,
            "DatabaseManager": MockDatabaseManager,
            "ConfigManager": MockConfigManager,
            "db": MockDatabaseManager.return_value,
            "scanner": MockCaseScanner.return_value,
            "workflow_engine": MockWorkflowEngine.return_value,
        }

        # Default behavior for a clean shutdown
        mocks["scanner"].observer.is_alive.return_value = True
        mocks["config"] = mock_config

        yield mocks


# --- Tests for RUNNING cases (largely unchanged) ---


def test_main_loop_handles_running_case_success(mock_dependencies):
    """Tests a 'running' case that completes successfully."""
    mocks = mock_dependencies
    now = datetime.now(timezone.utc).isoformat()
    running_case = {"case_id": 1, "pueue_task_id": 101, "status_updated_at": now}
    # In the refactored loop, we expect these calls in order:
    # 1. get_cases_by_status("submitting") -> from recover_stuck_submitting_cases
    # 2. get_cases_by_status("running")    -> from manage_running_cases
    # 3. get_resources_by_status("zombie") -> from manage_zombie_resources
    # 4. get_cases_by_status("submitted")  -> from process_new_submitted_cases
    mocks["db"].get_cases_by_status.side_effect = [
        [],  # For 'submitting'
        [running_case],  # For 'running'
        [],  # For 'submitted'
        SystemExit,  # Exit the loop
    ]
    mocks["db"].get_resources_by_status.return_value = []  # For 'zombie'
    mocks["submitter"].get_workflow_status.return_value = "success"

    with pytest.raises(SystemExit):
        main(mocks["config"])

    mocks["submitter"].get_workflow_status.assert_called_once_with(101)
    mocks["db"].update_case_completion.assert_called_once_with(1, status="completed")
    mocks["scanner"].stop.assert_called_once()


def test_main_loop_handles_running_case_failure(mock_dependencies):
    """Tests a 'running' case that fails."""
    mocks = mock_dependencies
    now = datetime.now(timezone.utc).isoformat()
    running_case = {"case_id": 2, "pueue_task_id": 102, "status_updated_at": now}
    # Adjust mock for the refactored loop structure
    mocks["db"].get_cases_by_status.side_effect = [
        [],  # For 'submitting'
        [running_case],  # For 'running'
        [],  # For 'submitted'
        SystemExit,  # Exit the loop
    ]
    mocks["db"].get_resources_by_status.return_value = []  # For 'zombie'
    mocks["submitter"].get_workflow_status.return_value = "failure"

    with pytest.raises(SystemExit):
        main(mocks["config"])

    mocks["submitter"].get_workflow_status.assert_called_once_with(102)
    mocks["db"].update_case_completion.assert_called_once_with(2, status="failed")


def test_main_loop_times_out_case_and_kill_succeeds(mock_dependencies):
    """
    Tests that when a case times out and the remote kill command succeeds,
    the case is marked as failed and the resource is released.
    """
    mocks = mock_dependencies
    mocks["config"]["main_loop"]["running_case_timeout_hours"] = 0.01
    from datetime import timedelta

    old_timestamp = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    timed_out_case = {
        "case_id": 3,
        "pueue_task_id": 103,
        "status_updated_at": old_timestamp,
    }

    mocks["db"].get_cases_by_status.side_effect = [[], [timed_out_case], SystemExit]
    mocks["submitter"].kill_workflow.return_value = True  # Simulate kill success

    with pytest.raises(SystemExit):
        main(mocks["config"])

    mocks["submitter"].get_workflow_status.assert_not_called()
    mocks["submitter"].kill_workflow.assert_called_once_with(103)
    mocks["db"].update_case_completion.assert_called_once_with(3, status="failed")
    mocks["db"].release_gpu_resource.assert_called_once_with(3)
    mocks["db"].update_gpu_status.assert_not_called()  # Should not become a zombie


def test_main_loop_times_out_case_and_kill_fails(mock_dependencies):
    """
    Tests that when a case times out and the remote kill command fails,
    the case is marked as failed and the resource is marked as 'zombie'.
    """
    mocks = mock_dependencies
    mocks["config"]["main_loop"]["running_case_timeout_hours"] = 0.01
    from datetime import timedelta

    old_timestamp = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    timed_out_case = {
        "case_id": 4,
        "pueue_task_id": 104,
        "pueue_group": "gpu_a",
        "status_updated_at": old_timestamp,
    }

    mocks["db"].get_cases_by_status.side_effect = [[], [timed_out_case], SystemExit]
    mocks["submitter"].kill_workflow.return_value = False  # Simulate kill failure

    with pytest.raises(SystemExit):
        main(mocks["config"])

    mocks["submitter"].kill_workflow.assert_called_once_with(104)
    mocks["db"].update_case_completion.assert_called_once_with(4, status="failed")
    mocks["db"].release_gpu_resource.assert_not_called()  # Should not be released
    mocks["db"].update_gpu_status.assert_called_once_with(
        "gpu_a", status="zombie", case_id=4
    )


def test_main_loop_recovers_zombie_resource(mock_dependencies):
    """
    Tests that the main loop finds zombie resources, attempts to kill their
    jobs, and releases them on success.
    """
    mocks = mock_dependencies
    zombie_resource = {"pueue_group": "gpu_b", "assigned_case_id": 5}
    failed_case = {"case_id": 5, "pueue_task_id": 105}

    # Loop 1: No stuck, no running, one zombie, no submitted. Loop 2: Clean exit.
    mocks["db"].get_cases_by_status.side_effect = [
        [],
        [],
        [],
        SystemExit,
    ]  # No other cases
    mocks["db"].get_resources_by_status.return_value = [zombie_resource]
    mocks["db"].get_case_by_id.return_value = failed_case
    mocks["submitter"].kill_workflow.return_value = True  # Kill now succeeds

    with pytest.raises(SystemExit):
        main(mocks["config"])

    mocks["db"].get_resources_by_status.assert_called_once_with("zombie")
    mocks["db"].get_case_by_id.assert_called_once_with(5)
    mocks["submitter"].kill_workflow.assert_called_once_with(105)
    mocks["db"].release_gpu_resource.assert_called_once_with(5)


# --- Tests for SUBMITTED cases (rewritten for dynamic allocation) ---


def test_main_loop_submits_case_with_available_gpu(mock_dependencies):
    """
    Tests the new dynamic submission process:
    1. A submitted case is found.
    2. An available GPU is found and locked.
    3. The case is submitted to that GPU's group.
    """
    mocks = mock_dependencies
    submitted_case = {"case_id": 4, "case_path": "/path/new"}
    # Loop 1: No stuck, no running, one submitted. Loop 2: Clean exit.
    mocks["db"].get_cases_by_status.side_effect = [[], [], [submitted_case], SystemExit]

    # Simulate that no resource is currently assigned
    mocks["db"].get_gpu_resource_by_case_id.return_value = None

    # Simulate a successful GPU lock
    mocks["db"].find_and_lock_any_available_gpu.return_value = "gpu_b"
    mocks["submitter"].submit_workflow.return_value = 201

    with pytest.raises(SystemExit):
        main(mocks["config"])

    # Verify the dynamic allocation logic
    mocks["db"].find_and_lock_any_available_gpu.assert_called_once_with(4)
    mocks["db"].update_case_pueue_group.assert_called_once_with(4, "gpu_b")

    # Verify the submission
    mocks["submitter"].submit_workflow.assert_called_once_with(
        case_id=4, case_path="/path/new", pueue_group="gpu_b"
    )
    mocks["db"].update_case_pueue_task_id.assert_called_once_with(4, 201)

    # Verify status updates
    assert (
        call(4, status="submitting", progress=10)
        in mocks["db"].update_case_status.call_args_list
    )
    assert (
        call(4, status="running", progress=30)
        in mocks["db"].update_case_status.call_args_list
    )


def test_main_loop_defers_submission_when_no_gpu_available(mock_dependencies):
    """
    Tests that if no GPU is available, the case is not submitted and the system
    waits for the next cycle.
    """
    mocks = mock_dependencies
    submitted_case = {"case_id": 5, "case_path": "/path/wait"}
    # Loop 1: No stuck, no running, one submitted. Loop 2: Clean exit.
    mocks["db"].get_cases_by_status.side_effect = [[], [], [submitted_case], SystemExit]

    # Simulate that no resource is currently assigned
    mocks["db"].get_gpu_resource_by_case_id.return_value = None

    # Simulate NO available GPU
    mocks["db"].find_and_lock_any_available_gpu.return_value = None

    with pytest.raises(SystemExit):
        main(mocks["config"])

    # Verify we checked for a GPU
    mocks["db"].find_and_lock_any_available_gpu.assert_called_once_with(5)

    # CRITICAL: Verify no further action was taken
    mocks["db"].update_case_pueue_group.assert_not_called()
    mocks["submitter"].submit_workflow.assert_not_called()
    mocks["db"].update_case_status.assert_not_called()


def test_main_loop_handles_submission_id_failure(mock_dependencies):
    """
    Tests that if a workflow is submitted but parsing the ID fails,
    the case is marked as 'failed' and the GPU is released.
    """
    mocks = mock_dependencies
    submitted_case = {"case_id": 6, "case_path": "/path/id_fail"}
    # Loop 1: No stuck, no running, one submitted. Loop 2: Clean exit.
    mocks["db"].get_cases_by_status.side_effect = [[], [], [submitted_case], SystemExit]

    # Simulate that no resource is currently assigned
    mocks["db"].get_gpu_resource_by_case_id.return_value = None

    # Simulate a successful lock but a failed submission (no ID returned)
    mocks["db"].find_and_lock_any_available_gpu.return_value = "gpu_a"
    mocks["submitter"].submit_workflow.return_value = None

    with pytest.raises(SystemExit):
        main(mocks["config"])

    # Verify lock and submission attempt
    mocks["db"].find_and_lock_any_available_gpu.assert_called_once_with(6)
    mocks["submitter"].submit_workflow.assert_called_once_with(
        case_id=6, case_path="/path/id_fail", pueue_group="gpu_a"
    )

    # Verify failure handling
    mocks["db"].update_case_completion.assert_called_once_with(6, status="failed")
    mocks["db"].release_gpu_resource.assert_called_once_with(6)


def test_main_loop_recovers_stuck_submitting_case_correctly(mock_dependencies):
    """
    Tests the recovery logic for a case stuck in 'submitting' state.
    When a remote task is found, the case should be updated to 'running'
    and NOT marked as 'failed'.
    """
    mocks = mock_dependencies
    stuck_case = {"case_id": 7, "case_path": "/path/stuck"}
    remote_task = {"id": 301, "label": "mqic_case_7"}

    # Loop 1: One stuck, no running, no others. Loop 2: Clean exit.
    mocks["db"].get_cases_by_status.side_effect = [
        [stuck_case],  # For stuck 'submitting' cases
        [],  # For running cases timeout check
        [],  # For running cases status check
        [],  # For submitted cases
        SystemExit,
    ]
    # Simulate finding the task on the remote HPC
    mocks["submitter"].find_task_by_label.return_value = ("found", remote_task)

    with pytest.raises(SystemExit):
        main(mocks["config"])

    # Verify the check was made
    mocks["submitter"].find_task_by_label.assert_called_once_with("mqic_case_7")

    # Verify the CORRECT recovery action
    mocks["db"].update_case_pueue_task_id.assert_called_once_with(7, 301)
    mocks["db"].update_case_status.assert_called_once_with(
        7, status="running", progress=30
    )

    # CRITICAL: Verify the BUGGY actions are NOT taken
    mocks["db"].update_case_completion.assert_not_called()
    mocks["db"].release_gpu_resource.assert_not_called()
