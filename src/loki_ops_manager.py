"""Legacy helper for managing a Loki binary outside the charm runtime."""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import requests


class LokiManager:
    """Manage a standalone Loki installation for local helper scripts."""

    def __init__(self):
        self.loki_home = Path("/opt/loki")
        self.loki = Path("/opt/loki/loki-linux-amd64")
        self.loki_cfg = self.loki_home / "loki-local-config.yaml"
        self.loki_unitfile = Path("/etc/systemd/system/loki.service")

    def _prepare_os(self):
        """Prepare the local filesystem for Loki installation."""
        try:
            subprocess.run(["mkdir", "-p", self.loki_home], check=True)
            print(f"Prepared OS for loki installation {self.loki_home}")
        except Exception:  # noqa: BLE001
            print(f"Error preparing OS for loki installation {self.loki_home}")

    def _install_from_resource(self, resource_path):
        """Install Loki from a zip resource."""
        if self.loki_home.exists():
            shutil.rmtree(self.loki_home)

        with zipfile.ZipFile(resource_path, "r") as zip_ref:
            zip_ref.extractall(self.loki_home)

        try:
            subprocess.run(["chmod", "a+x", self.loki], check=True)
        except Exception:  # noqa: BLE001
            print("Error installing loki binary")
            sys.exit(1)

    def _install_config(self):
        """Install the local config from the template."""
        if self.loki_cfg.exists():
            self.loki_cfg.unlink()
        lokiconfig_tmpl = Path("templates/loki-local-config.yaml.tmpl").read_text()
        self.loki_cfg.write_text(lokiconfig_tmpl)

    def _install_systemd_unitfile(self):
        """Install the systemd unit file."""
        if self.loki_unitfile.exists():
            self.loki_unitfile.unlink()
        systemdunitfile_tmpl = Path("templates/loki.service.tmpl").read_text()
        self.loki_unitfile.write_text(systemdunitfile_tmpl)

    def stop_loki(self):
        """Stop Loki."""
        try:
            subprocess.run(["systemctl", "stop", "loki"], check=True)
        except Exception as exc:  # noqa: BLE001
            print("Error stopping loki", str(exc))

    def start_loki(self):
        """Start Loki."""
        try:
            subprocess.run(["systemctl", "start", "loki"], check=True)
        except Exception as exc:  # noqa: BLE001
            print("Error starting loki", str(exc))

    def restart_loki(self):
        """Restart Loki."""
        try:
            subprocess.run(["systemctl", "restart", "loki"], check=True)
        except Exception as exc:  # noqa: BLE001
            print("Error starting loki", str(exc))

    def install(self, resource_file):
        """Install Loki from a supplied zip resource."""
        self._prepare_os()
        self._install_from_resource(resource_file)
        self._install_config()
        self._install_systemd_unitfile()

    def loki_version(self):
        """Return the Loki version string, or `None` on failure."""
        try:
            output = subprocess.run(
                [
                    self.loki.resolve(),
                    "-config.file",
                    self.loki_cfg.resolve(),
                    "-version",
                ],
                capture_output=True,
                check=False,
            ).stdout.decode()
            match = re.search(r"version\s*([\d.]+)", output)
            return match.group(1) if match else None
        except Exception as exc:  # noqa: BLE001
            print("Error getting version from loki", exc)
            return None

    def verify_config(self, filename=None):
        """Use Loki to verify a config and return the validation match object."""
        file_to_check = Path(filename) if filename else self.loki_cfg
        try:
            result = subprocess.run(
                [
                    self.loki.resolve(),
                    "-config.file",
                    file_to_check.resolve(),
                    "-verify-config",
                ],
                capture_output=True,
                check=False,
            )
            return re.search(r"config is valid", result.stderr.decode())
        except Exception as exc:  # noqa: BLE001
            print("Error verifying config", exc)
            return None

    def is_ready(self):
        """Check whether Loki reports ready on the local HTTP endpoint."""
        response = requests.get("http://localhost:3100/ready", timeout=2.50)
        return response.text.strip() == "ready"

    def _purge(self):
        """Wipe the installation and remove all traces of Loki."""
        try:
            subprocess.run(["rm", self.loki], check=True)
            print("Success removing loki bin", self.loki)
            subprocess.run(["rm", self.loki_unitfile], check=True)
            print("Success removing loki unitfile", self.loki_unitfile)
            subprocess.run(["rm", "-rf", self.loki_home], check=True)
            print("Success purging loki home dir", self.loki_home)
        except Exception:  # noqa: BLE001
            print("Error purging loki")
