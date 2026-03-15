from src.monitoring.run_tracker import get_run_history, record_run
from src.storage.client import get_storage_client


def test_record_run_and_history():
    s3 = get_storage_client()
    key = "metadata/runs/history.jsonl"
    if s3.exists(key):
        s3.delete_object(key)

    record_run("test.step", status="success", rows=42, duration_seconds=1.5)
    history = get_run_history()
    assert len(history) >= 1

    last = history[-1]
    assert last["step"] == "test.step"
    assert last["status"] == "success"
    assert last["rows"] == 42


def test_record_failure():
    record_run("test.fail", status="failure", error="boom")
    history = get_run_history(step="test.fail")
    assert any(r["status"] == "failure" and r["error"] == "boom" for r in history)
