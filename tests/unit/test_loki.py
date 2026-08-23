# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

import subprocess
from email.message import Message
from pathlib import Path

import pytest

import loki


def test_get_version_parses_output(monkeypatch: pytest.MonkeyPatch):
    def mock_run(cmd, timeout=None):
        return subprocess.CompletedProcess(cmd, 0, stdout="loki, version 2.9.0", stderr="")

    monkeypatch.setattr(loki, "_run", mock_run)

    assert loki.get_version() == "2.9.0"


def test_get_version_returns_none_on_error(monkeypatch: pytest.MonkeyPatch):
    def mock_run(cmd, timeout=None):
        raise subprocess.CalledProcessError(1, cmd)

    monkeypatch.setattr(loki, "_run", mock_run)

    assert loki.get_version() is None


def test_write_config_writes_files(tmp_path: Path):
    config_path = tmp_path / "config.yml"
    backup_path = tmp_path / "config.yml.bak"

    loki.write_config({"a": 1}, config_path=config_path, backup_path=backup_path)

    assert config_path.exists()
    assert backup_path.exists()
    assert config_path.read_text().strip() == "a: 1"


def test_verify_config_invokes_loki(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    seen = {}

    def mock_run(cmd, timeout=None):
        seen["cmd"] = cmd
        seen["timeout"] = timeout
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    monkeypatch.setattr(loki, "_run", mock_run)
    config_path = tmp_path / "config.yml"
    config_path.write_text("a: 1")

    loki.verify_config(config_path=config_path, timeout=12)

    assert seen["cmd"][0] == "loki"
    assert str(config_path) in seen["cmd"]
    assert seen["timeout"] == 12


def test_check_ready_returns_true_for_ready_response(monkeypatch: pytest.MonkeyPatch):
    class _Response:
        status = 200

        def read(self):
            return b"ready"

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(loki.request, "urlopen", lambda *args, **kwargs: _Response())

    ready, error = loki.check_ready("http://localhost:3100")

    assert ready is True
    assert error is None


def test_check_ready_returns_error_for_non_ready_response(monkeypatch: pytest.MonkeyPatch):
    class _Response:
        status = 503

        def read(self):
            return b"not ready"

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(loki.request, "urlopen", lambda *args, **kwargs: _Response())

    ready, error = loki.check_ready("http://localhost:3100")

    assert ready is False
    assert error == "HTTP 503: not ready"


def test_check_endpoint_returns_true_for_2xx_response(monkeypatch: pytest.MonkeyPatch):
    class _Response:
        status = 204

        def read(self):
            return b""

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(loki.request, "urlopen", lambda *args, **kwargs: _Response())

    ready, error = loki.check_endpoint("http://example:9000")

    assert ready is True
    assert error is None


def test_check_endpoint_returns_error_for_http_error(monkeypatch: pytest.MonkeyPatch):
    def _raise(*args, **kwargs):
        raise loki.error.HTTPError(
            url="http://example:9000",
            code=503,
            msg="Service Unavailable",
            hdrs=Message(),
            fp=None,
        )

    monkeypatch.setattr(loki.request, "urlopen", _raise)

    ready, error = loki.check_endpoint("http://example:9000")

    assert ready is False
    assert error == "HTTP 503: Service Unavailable"


def test_check_s3_endpoint_accepts_only_expected_anonymous_auth_challenge(
    monkeypatch: pytest.MonkeyPatch,
):
    def _raise(*args, **kwargs):
        raise loki.error.HTTPError(
            url="http://example:9000",
            code=403,
            msg="Forbidden",
            hdrs=Message(),
            fp=None,
        )

    monkeypatch.setattr(loki.request, "urlopen", _raise)

    reachable, error = loki.check_s3_endpoint("http://example:9000")

    assert reachable is True
    assert error is None


def test_check_s3_endpoint_rejects_other_http_errors(monkeypatch: pytest.MonkeyPatch):
    def _raise(*args, **kwargs):
        raise loki.error.HTTPError(
            url="http://example:9000",
            code=404,
            msg="Not Found",
            hdrs=Message(),
            fp=None,
        )

    monkeypatch.setattr(loki.request, "urlopen", _raise)

    reachable, error = loki.check_s3_endpoint("http://example:9000")

    assert reachable is False
    assert error == "HTTP 404: Not Found"


def test_ensure_data_dir_symlink_default(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    default_dir = tmp_path / "default"
    target_dir = tmp_path / "target"

    monkeypatch.setattr(loki, "DEFAULT_DATA_DIR", str(default_dir))

    loki.ensure_data_dir(str(target_dir))

    assert default_dir.is_symlink()
    assert default_dir.resolve() == target_dir


def test_ensure_data_dir_moves_existing_data(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    default_dir = tmp_path / "default"
    target_dir = tmp_path / "target"
    default_dir.mkdir(parents=True)
    (default_dir / "chunks").write_text("data")

    monkeypatch.setattr(loki, "DEFAULT_DATA_DIR", str(default_dir))

    loki.ensure_data_dir(str(target_dir))

    assert (target_dir / "chunks").read_text() == "data"
    assert default_dir.is_symlink()
    assert default_dir.resolve() == target_dir


def test_ensure_data_dir_resets_to_default(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    default_dir = tmp_path / "default"
    target_dir = tmp_path / "target"
    default_dir.parent.mkdir(parents=True, exist_ok=True)
    default_dir.symlink_to(target_dir)

    monkeypatch.setattr(loki, "DEFAULT_DATA_DIR", str(default_dir))

    loki.ensure_data_dir(str(default_dir))

    assert default_dir.exists()
    assert not default_dir.is_symlink()
