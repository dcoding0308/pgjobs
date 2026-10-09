import pytest

from pgjobs import ConfigurationError, JobApp
from pgjobs.cli import load_app, main, resolve_database_url

app = JobApp("postgresql://unused/unused")
not_an_app = object()


def test_load_app_returns_the_instance() -> None:
    assert load_app(f"{__name__}:app") is app


@pytest.mark.parametrize(
    "path",
    ["no_colon", ":app", f"{__name__}:", "does.not.exist:app", f"{__name__}:not_an_app"],
)
def test_load_app_rejects_bad_paths(path: str) -> None:
    with pytest.raises(ConfigurationError):
        load_app(path)


def test_database_url_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://generic")
    assert resolve_database_url(None) == "postgresql://generic"
    monkeypatch.setenv("PGJOBS_DATABASE_URL", "postgresql://specific")
    assert resolve_database_url(None) == "postgresql://specific"
    assert resolve_database_url("postgresql://flag") == "postgresql://flag"


def test_missing_database_url_is_a_clean_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.delenv("PGJOBS_DATABASE_URL", raising=False)
    assert main(["stats"]) == 2
    assert "no database URL" in capsys.readouterr().err
