from src.app.run_paper import _guard_result


def test_guard_errors_threshold_allows_less_than_or_equal():
    thresholds = {"errors": 1}
    passed0, _ = _guard_result({"errors": 0}, thresholds, only={"errors"})
    passed1, _ = _guard_result({"errors": 1}, thresholds, only={"errors"})
    assert passed0 is True
    assert passed1 is True


def test_guard_errors_threshold_blocks_greater_than_limit():
    thresholds = {"errors": 1}
    passed, reason = _guard_result({"errors": 2}, thresholds, only={"errors"})
    assert passed is False
    assert "threshold_fail:errors" in reason
