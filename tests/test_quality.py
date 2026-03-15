from src.quality.checks import QualityResult


def test_quality_result_dataclass():
    r = QualityResult(
        check_name="test_check",
        passed=True,
        severity="info",
        details="All good",
        records_checked=100,
        records_failed=0,
    )
    assert r.passed is True
    assert r.check_name == "test_check"
    assert r.records_failed == 0
