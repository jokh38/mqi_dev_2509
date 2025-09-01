import pytest
import sqlite3
import os
from datetime import datetime
from typing import Generator

from src.common.db_manager import DatabaseManager

# Define the path for the test database
TEST_DB_PATH = "test_communicator.db"


@pytest.fixture
def db_manager() -> Generator[DatabaseManager, None, None]:
    """
    Pytest fixture to set up and tear down a test database.
    This fixture creates a new DatabaseManager instance and initializes
    a fresh database for each test function.
    After the test runs, it removes the test database file.
    """
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)

    manager = DatabaseManager(db_path=TEST_DB_PATH)
    manager.init_db()

    yield manager

    manager.close()
    if os.path.exists(TEST_DB_PATH):
        os.remove(TEST_DB_PATH)


# --- Test Suite ---


def test_database_initialization(db_manager: DatabaseManager):
    """
    Tests if the database and the required tables ('cases' and 'gpu_resources')
    are created successfully.
    """
    assert os.path.exists(TEST_DB_PATH)
    conn = sqlite3.connect(TEST_DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='cases';"
    )
    assert cursor.fetchone() is not None, "'cases' table was not created."
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='gpu_resources';"
    )
    assert cursor.fetchone() is not None, "'gpu_resources' table was not created."
    conn.close()


def test_add_and_get_case(db_manager: DatabaseManager):
    """
    Tests adding a new case (without a pueue_group) and retrieving it.
    """
    case_path = "/path/to/case1"
    case_id = db_manager.add_case(case_path=case_path)
    assert case_id is not None and case_id > 0

    case = db_manager.get_case_by_id(case_id)
    assert case is not None
    assert case["case_id"] == case_id
    assert case["case_path"] == case_path
    assert case["status"] == "submitted"
    assert case["progress"] == 0
    assert case["pueue_group"] is None  # Should be NULL initially
    assert "submitted_at" in case
    assert datetime.fromisoformat(case["submitted_at"]).tzinfo is not None


def test_update_case_pueue_group(db_manager: DatabaseManager):
    """
    Tests assigning a pueue_group to a case.
    """
    case_id = db_manager.add_case("/path/to/case_for_group_update")
    assert case_id is not None

    # Check that it's initially NULL
    case = db_manager.get_case_by_id(case_id)
    assert case is not None
    assert case["pueue_group"] is None

    # Update the group
    new_group = "gpu_a"
    db_manager.update_case_pueue_group(case_id, new_group)

    # Verify the update
    case = db_manager.get_case_by_id(case_id)
    assert case is not None
    assert case["pueue_group"] == new_group


def test_find_and_lock_any_available_gpu(db_manager: DatabaseManager):
    """
    Tests the core logic of finding and locking any available GPU.
    """
    # Setup: one case and multiple GPU resources
    case_id = db_manager.add_case("/path/to/case_for_locking")
    assert case_id is not None
    db_manager.ensure_gpu_resource_exists("gpu_a")
    db_manager.ensure_gpu_resource_exists("gpu_b")

    # Action: find and lock
    locked_group = db_manager.find_and_lock_any_available_gpu(case_id)
    assert locked_group == "gpu_a"  # Should pick the first one

    # Verification
    locked_resource = db_manager.get_gpu_resource("gpu_a")
    assert locked_resource is not None
    assert locked_resource["status"] == "assigned"
    assert locked_resource["assigned_case_id"] == case_id

    # Ensure the other resource is untouched
    other_resource = db_manager.get_gpu_resource("gpu_b")
    assert other_resource is not None
    assert other_resource["status"] == "available"


def test_find_and_lock_gpu_when_first_is_busy(db_manager: DatabaseManager):
    """
    Tests that the locking mechanism skips busy GPUs and finds the next available one.
    """
    case_id_1 = db_manager.add_case("/path/to/case1")
    case_id_2 = db_manager.add_case("/path/to/case2")
    assert case_id_1 is not None and case_id_2 is not None

    db_manager.ensure_gpu_resource_exists("gpu_a")
    db_manager.update_gpu_status("gpu_a", "assigned", case_id_1)
    db_manager.ensure_gpu_resource_exists("gpu_b")

    # Action: should skip gpu_a and lock gpu_b
    locked_group = db_manager.find_and_lock_any_available_gpu(case_id_2)
    assert locked_group == "gpu_b"

    # Verification
    resource_b = db_manager.get_gpu_resource("gpu_b")
    assert resource_b is not None
    assert resource_b["status"] == "assigned"
    assert resource_b["assigned_case_id"] == case_id_2


def test_find_and_lock_returns_none_when_all_gpus_busy(db_manager: DatabaseManager):
    """
    Tests that the locking mechanism returns None when no GPUs are available.
    """
    case_id = db_manager.add_case("/path/to/case_no_gpus")
    assert case_id is not None
    db_manager.ensure_gpu_resource_exists("gpu_a")
    db_manager.update_gpu_status("gpu_a", "assigned", case_id)
    db_manager.ensure_gpu_resource_exists("gpu_b")
    db_manager.update_gpu_status("gpu_b", "assigned", case_id)

    # Action: try to lock a resource
    locked_group = db_manager.find_and_lock_any_available_gpu(case_id)

    # Verification
    assert locked_group is None


def test_release_gpu_resource(db_manager: DatabaseManager):
    """
    Tests that releasing a resource makes it available again.
    """
    # Setup: a locked resource
    case_id = db_manager.add_case("/path/to/case_for_release")
    assert case_id is not None
    db_manager.ensure_gpu_resource_exists("gpu_a")
    locked_group = db_manager.find_and_lock_any_available_gpu(case_id)
    assert locked_group == "gpu_a"

    # Verify it's locked
    resource = db_manager.get_gpu_resource("gpu_a")
    assert resource is not None
    assert resource["status"] == "assigned"
    assert resource["assigned_case_id"] == case_id

    # Action: release the resource
    db_manager.release_gpu_resource(case_id)

    # Verification: resource should now be available
    resource = db_manager.get_gpu_resource("gpu_a")
    assert resource is not None
    assert resource["status"] == "available"
    assert resource["assigned_case_id"] is None


def test_ensure_gpu_resource_exists(db_manager: DatabaseManager):
    """
    Tests that a GPU resource is created only if it doesn't already exist.
    """
    db_manager.ensure_gpu_resource_exists("gpu_new")
    resource = db_manager.get_gpu_resource("gpu_new")
    assert resource is not None
    assert resource["status"] == "available"

    # Call it again, it should not fail or create a duplicate
    db_manager.ensure_gpu_resource_exists("gpu_new")
    # A more robust test would check the count, but this is sufficient for now

    # Let's check if we can assign it
    case_id = db_manager.add_case("/path/to/case_for_ensure")
    assert case_id is not None
    db_manager.update_gpu_status("gpu_new", "assigned", case_id)
    resource = db_manager.get_gpu_resource("gpu_new")
    assert resource["status"] == "assigned"

    # Now call ensure again, it should NOT reset the status to available
    db_manager.ensure_gpu_resource_exists("gpu_new")
    resource = db_manager.get_gpu_resource("gpu_new")
    assert resource["status"] == "assigned"


# Keep other tests that are still relevant and correct
def test_get_case_by_path(db_manager: DatabaseManager):
    case_path = "/path/to/unique_case"
    case_id = db_manager.add_case(case_path)
    case = db_manager.get_case_by_path(case_path)
    assert case is not None
    assert case["case_id"] == case_id


def test_get_cases_by_status(db_manager: DatabaseManager):
    id1 = db_manager.add_case("/path/case_submitted_1")
    id2 = db_manager.add_case("/path/case_submitted_2")
    id3 = db_manager.add_case("/path/case_running")

    db_manager.update_case_status(id3, "running", 50)
    db_manager.update_case_completion(id2, "completed")

    submitted = db_manager.get_cases_by_status("submitted")
    assert len(submitted) == 1
    assert submitted[0]["case_id"] == id1

    running = db_manager.get_cases_by_status("running")
    assert len(running) == 1
    assert running[0]["case_id"] == id3


def test_update_case_completion_preserves_historical_data(db_manager: DatabaseManager):
    """
    Tests that update_case_completion correctly marks a case as complete
    AND PRESERVES the pueue_group and pueue_task_id fields for historical tracking.
    """
    # 1. Setup a case as if it were running
    case_id = db_manager.add_case("/path/to/completed_case")
    assert case_id is not None
    db_manager.update_case_pueue_group(case_id, "gpu_a")
    db_manager.update_case_pueue_task_id(case_id, 12345)
    db_manager.update_case_status(case_id, "running", 50)

    # Verify it's in a running-like state
    case = db_manager.get_case_by_id(case_id)
    assert case is not None
    assert case["pueue_group"] == "gpu_a"
    assert case["pueue_task_id"] == 12345
    assert case["status"] == "running"

    # 2. Action: Mark the case as completed
    db_manager.update_case_completion(case_id, "completed")

    # 3. Verification
    completed_case = db_manager.get_case_by_id(case_id)
    assert completed_case is not None
    assert completed_case["status"] == "completed"
    assert completed_case["progress"] == 100
    assert completed_case["completed_at"] is not None
    # Verify that historical data is PRESERVED
    assert completed_case["pueue_group"] == "gpu_a"
    assert completed_case["pueue_task_id"] == 12345


def test_get_resources_by_status(db_manager: DatabaseManager):
    """
    Tests retrieving GPU resources based on their status.
    """
    db_manager.ensure_gpu_resource_exists("gpu_a")
    case_id = db_manager.add_case("/path/to/case_for_status")
    assert case_id is not None
    db_manager.update_gpu_status("gpu_b", "assigned", case_id)
    db_manager.ensure_gpu_resource_exists("gpu_c")
    db_manager.update_gpu_status("gpu_c", "zombie", case_id)
    db_manager.ensure_gpu_resource_exists("gpu_d")

    available = db_manager.get_resources_by_status("available")
    assert len(available) >= 2
    assert "gpu_a" in [r["pueue_group"] for r in available]
    assert "gpu_d" in [r["pueue_group"] for r in available]

    assigned = db_manager.get_resources_by_status("assigned")
    assert len(assigned) == 1
    assert assigned[0]["pueue_group"] == "gpu_b"

    zombie = db_manager.get_resources_by_status("zombie")
    assert len(zombie) == 1
    assert zombie[0]["pueue_group"] == "gpu_c"

    empty = db_manager.get_resources_by_status("non_existent_status")
    assert len(empty) == 0
