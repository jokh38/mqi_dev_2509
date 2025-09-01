import subprocess
from unittest.mock import patch, MagicMock
import pytest
import json
from src.services.workflow_engine import (
    WorkflowEngine,
    WorkflowExecutionError,
)


@pytest.fixture
def mock_config():
    """Fixture to provide a mock configuration for tests."""
    return {
        "hpc": {
            "host": "test_host",
            "user": "test_user",
            "remote_base_dir": "/remote/base/dir",
            "remote_command": "python sim.py",
        }
    }


class TestWorkflowEngine:
    """Test suite for the WorkflowEngine class."""

    def test_initialization(self, mock_config):
        """Test that WorkflowEngine initializes correctly."""
        engine = WorkflowEngine(config=mock_config)
        assert engine.hpc_config == mock_config["hpc"]

    def test_submit_workflow_success_returns_task_id(self, mock_config):
        """Test the successful submission of a workflow returns the parsed task ID."""
        engine = WorkflowEngine(config=mock_config)
        case_path = "/local/path/case_001"

        with patch("subprocess.run") as mock_run:
            # Simulate successful mkdir, scp, and 3 ssh commands for workflow steps
            mock_run.side_effect = [
                MagicMock(returncode=0, stdout="", stderr=""),  # mkdir
                MagicMock(returncode=0, stdout="Success", stderr=""),  # scp
                MagicMock(returncode=0, stdout="New task added (id: 101).", stderr=""),  # step 1
                MagicMock(returncode=0, stdout="New task added (id: 102).", stderr=""),  # step 2
                import subprocess
from unittest.mock import patch, MagicMock
import pytest
import json
from src.services.workflow_engine import (
    WorkflowEngine,
    WorkflowExecutionError,
)


@pytest.fixture
def mock_config():
    """Fixture to provide a mock configuration for tests."""
    return {
        "hpc": {
            "host": "test_host",
            "user": "test_user",
            "remote_base_dir": "/remote/base/dir",
            "remote_command": "python sim.py",
        }
    }


class TestWorkflowEngine:
    """Test suite for the WorkflowEngine class."""

    def test_initialization(self, mock_config):
        """Test that WorkflowEngine initializes correctly."""
        engine = WorkflowEngine(config=mock_config)
        assert engine.hpc_config == mock_config["hpc"]

    def test_process_case_success(self, mock_config):
        """Test the successful processing of a case."""
        engine = WorkflowEngine(config=mock_config)
        case_path = "/local/path/case_001"

        with patch.object(engine, '_execute_workflow_step', return_value=True) as mock_execute:
            result = engine.process_case(
                case_id=1, case_path=case_path, pueue_group="test_group"
            )

            assert result is True
            mock_execute.assert_called()

    def test_process_case_step_failure(self, mock_config):
        """Test that workflow processing fails if a step fails."""
        engine = WorkflowEngine(config=mock_config)
        case_path = "/local/path/case_001"

        with patch.object(engine, '_execute_workflow_step', return_value=False) as mock_execute:
            result = engine.process_case(case_id=1, case_path=case_path)

            assert result is False
            mock_execute.assert_called_once()
,  # step 3
            ]

            task_id = engine.submit_workflow(
                case_id=1, case_path=case_path, pueue_group="test_group"
            )

            assert task_id == 123  # Should return the final task ID
            assert mock_run.call_count == 5  # mkdir + scp + 3 workflow steps

    def test_submit_workflow_scp_failure(self, mock_config):
        """Test that workflow submission fails if scp command fails."""
        engine = WorkflowEngine(config=mock_config)
        case_path = "/local/path/case_001"

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(
                returncode=1, cmd="scp", stderr="SCP failed"
            )

            with pytest.raises(WorkflowExecutionError, match="Failed to create remote directory"):
                engine.submit_workflow(case_id=1, case_path=case_path)
            mock_run.assert_called_once()

    def test_submit_workflow_ssh_failure(self, mock_config):
        """Test that workflow submission fails if ssh command fails."""
        engine = WorkflowEngine(config=mock_config)
        case_path = "/local/path/case_001"

        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0),  # mkdir
                subprocess.CalledProcessError(
                    returncode=1, cmd="scp", stderr="SSH failed"
                ),
            ]

            with pytest.raises(WorkflowExecutionError, match="Failed to copy case"):
                engine.submit_workflow(case_id=1, case_path=case_path)
            assert mock_run.call_count == 2


class TestGetWorkflowStatus:
    """Test suite for the get_workflow_status method."""

    @pytest.fixture
    def engine(self, mock_config):
        return WorkflowEngine(config=mock_config)

    def mock_pueue_status(self, tasks_dict):
        """Helper to create a mock subprocess result with a given tasks dictionary."""
        json_output = json.dumps({"tasks": tasks_dict})
        return MagicMock(returncode=0, stdout=json_output, stderr="")

    def test_get_status_success(self, engine):
        """Test status is 'success' for a 'Done' task."""
        with patch("subprocess.run") as mock_run:
            # A 'Done' task must also have a 'success' result to be considered successful
            mock_run.return_value = self.mock_pueue_status(
                {"101": {"status": "Done", "result": "success"}}
            )
            status = engine.get_workflow_status(101)
            assert status == "success"

    def test_get_status_failure(self, engine):
        """Test status is 'failure' for a 'Failed' task."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = self.mock_pueue_status(
                {"102": {"status": "Failed"}}
            )
            status = engine.get_workflow_status(102)
            assert status == "failure"

    def test_get_status_running(self, engine):
        """Test status is 'running' for a 'Running' task."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = self.mock_pueue_status(
                {"103": {"status": "Running"}}
            )
            status = engine.get_workflow_status(103)
            assert status == "running"

    def test_get_status_queued_is_running(self, engine):
        """Test status is 'running' for a 'Queued' task."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = self.mock_pueue_status(
                {"104": {"status": "Queued"}}
            )
            status = engine.get_workflow_status(104)
            assert status == "running"

    def test_get_status_not_found(self, engine):
        """Test status is 'not_found' when the task ID is not in the output."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = self.mock_pueue_status({"200": {"status": "Done"}})
            status = engine.get_workflow_status(105)
            assert status == "not_found"

    def test_get_status_ssh_failure_is_unreachable(self, engine):
        """Test status is 'unreachable' if the ssh command fails."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.CalledProcessError(1, "ssh")
            status = engine.get_workflow_status(106)
            assert status == "unreachable"

    def test_get_status_timeout_is_unreachable(self, engine):
        """Test status is 'unreachable' if the ssh command times out."""
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = subprocess.TimeoutExpired("ssh", timeout=60)
            status = engine.get_workflow_status(107)
            assert status == "unreachable"

    def test_get_status_json_error_is_unreachable(self, engine):
        """Test status is 'unreachable' if the output is not valid JSON."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0, stdout="not json", stderr=""
            )
            status = engine.get_workflow_status(107)
            assert status == "unreachable"

    def test_get_status_done_with_failure_result(self, engine):
        """Test status is 'failure' for a 'Done' task with a 'failure' result."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = self.mock_pueue_status(
                {"108": {"status": "Done", "result": "failure"}}
            )
            status = engine.get_workflow_status(108)
            assert status == "failure"