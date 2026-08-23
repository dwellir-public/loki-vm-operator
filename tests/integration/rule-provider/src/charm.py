#!/usr/bin/env python3
# Copyright 2026 Erik Lönroth
# See LICENSE file for licensing details.

"""Test-only charm that publishes configured Loki rule relation data."""

import ops


class LokiRuleProviderTestCharm(ops.CharmBase):
    """Publish one complete standard alert_rules snapshot to every relation."""

    def __init__(self, framework: ops.Framework):
        """Observe config and relation events that may change the snapshot."""
        super().__init__(framework)
        framework.observe(self.on.config_changed, self._publish)
        framework.observe(self.on.send_loki_logs_relation_joined, self._publish)
        framework.observe(self.on.send_loki_logs_relation_changed, self._publish)

    def _publish(self, _event: ops.EventBase) -> None:
        """Replace application relation data with the configured snapshot."""
        if not self.unit.is_leader():
            return
        snapshot = str(self.config["alert-rules"])
        for relation in self.model.relations.get("send-loki-logs", []):
            if bool(self.config["omit-alert-rules"]):
                relation.data[self.app].pop("alert_rules", None)
            else:
                relation.data[self.app]["alert_rules"] = snapshot
        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":
    ops.main(LokiRuleProviderTestCharm)
