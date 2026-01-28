# Decisions to Take

* Clustering approach for VM units (memberlist vs external KV store) and required ports/firewall rules.
Decision: Memberlist
* Meaning of the `ingress` relation for this VM charm (Traefik per-unit, nginx-ingress-integrator, or direct endpoint).
Decision: No decision taken yet.
* Final on-disk paths and ownership for config/data/logs (including systemd unit expectations).
Decision: For persisting data, use `loki-persisted`, if not specified use default from apt package.
Decision: For config, use `/etc/loki/loki.yaml`.
Decision: For logs, use `/var/log/loki`.
Decision: For systemd unit, use `loki.service`.
* Version pinning policy for Loki upgrades from the Grafana APT repository.
Decision: No decision taken
