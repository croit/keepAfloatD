# keepAfloatD documentation

Start with the [project README](../README.md) for what it is and a quick start. Then, by role:

- **Operators / system administrators** — [operations.md](operations.md): running the daemon,
  observing which node holds a VIP, changing the VIP list, rolling upgrades, securing the cluster,
  and a troubleshooting section for the common failure modes.
- **Contributors / developers** — [development.md](development.md): building and testing, the code
  layout, the end-to-end scenario harness and how to add a scenario.

For the internals — the Raft consensus model, VIP fencing with per-VIP generations, and the failover
paths — see [architecture.md](architecture.md).
