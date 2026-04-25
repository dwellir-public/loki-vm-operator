# Releasing to charmhub

Login to the user: dwellir-snapcrafters

```bash
charmcraft login 
```

```bash
charmcraft pack 
charmcraft upload loki-vm_amd64.charm
charmcraft release loki-vm -r <release-number> -c edge
```
