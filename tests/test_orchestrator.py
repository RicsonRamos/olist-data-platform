import pytest
from unittest.mock import MagicMock
from pipeline.orchestrator import Orchestrator, Task

def test_task_success():
    func = MagicMock(return_value="done")
    task = Task("test", func, retries=1)
    
    result = task.run()
    
    assert result == "done"
    assert task.status == "SUCCESS"
    assert func.call_count == 1

def test_task_retry_and_success():
    func = MagicMock()
    # Fail once, then succeed
    func.side_effect = [Exception("fail"), "done"]
    task = Task("test", func, retries=1, retry_delay=0)
    
    result = task.run()
    
    assert result == "done"
    assert func.call_count == 2
    assert task.status == "SUCCESS"

def test_task_failure():
    func = MagicMock(side_effect=Exception("permanent fail"))
    task = Task("test", func, retries=1, retry_delay=0)
    
    with pytest.raises(Exception, match="permanent fail"):
        task.run()
    
    assert task.status == "FAILED"
    assert func.call_count == 2

def test_orchestrator_run_all():
    orc = Orchestrator()
    t1 = orc.add_task("t1", MagicMock(return_value=True))
    t2 = orc.add_task("t2", MagicMock(return_value=True), dependencies=["t1"])
    
    orc.run_all()
    
    assert t1.status == "SUCCESS"
    assert t2.status == "SUCCESS"

def test_orchestrator_dependency_failure():
    orc = Orchestrator()
    t1 = orc.add_task("t1", MagicMock(side_effect=Exception("fail")), retries=0)
    t2 = orc.add_task("t2", MagicMock(), dependencies=["t1"])
    
    orc.run_all()
    
    assert t1.status == "FAILED"
    # Note: in the current implementation, it breaks the loop if a task fails.
    # So t2 remains PENDING or is not reached.
