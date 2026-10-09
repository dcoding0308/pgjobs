from pgjobs.errors import summarize_error


def test_summarize_error_returns_last_traceback_line() -> None:
    traceback_text = "Traceback (most recent call last):\n  File x\nValueError: bad input\n\n"
    assert summarize_error(traceback_text) == "ValueError: bad input"


def test_summarize_error_handles_empty_values() -> None:
    assert summarize_error(None) is None
    assert summarize_error("   \n ") is None


def test_summarize_error_truncates() -> None:
    assert summarize_error("x" * 1000, max_length=10) == "x" * 10
