# How to Run HAProxy in Active/Active/Active with keepAfloatD

High availability for virtual IPs usually starts with a familiar pattern: one active node, one or more standby nodes, and a failover event when the active side disappears. That model works, and for many small environments it is still enough. But it also leaves resources idle. If you already run three HAProxy nodes, it is natural to ask a simple question: why should two of them spend most of their time waiting?

That is exactly the problem we built `keepAfloatD` to solve. `keepAfloatD` is a small Rust daemon that provides Keepalived-like VIP failover, but uses Raft consensus instead of VRRP to decide who should own each virtual IP. The result is a deterministic, multi-active model where several healthy nodes can host traffic at the same time.

In this post, we will look at what `keepAfloatD` does, how it differs from classic active/passive failover, and how you can use it to build a three-node HAProxy cluster where all three nodes actively participate.

<!-- Image suggestion: 3-node HAProxy cluster with keepAfloatD on each node and three VIPs distributed across the cluster -->

## Why active/passive is often not enough

Classic active/passive failover is easy to understand. One node owns the VIP. The others are ready to take over if that node fails. This is a good fit for simple environments, but it also means your standby nodes are mostly idle during normal operation.

That trade-off becomes more obvious when you run HAProxy. HAProxy is designed to serve traffic efficiently, and many teams already deploy it on several nodes for resilience. In an active/passive VIP design, though, you still end up with one node carrying the public traffic while the others mostly wait for something to go wrong.

A multi-active design changes that picture. Instead of assigning all VIP ownership to one node, you distribute multiple VIPs across several healthy nodes. Each node carries part of the load. If one node becomes unhealthy, the remaining nodes absorb its VIPs and keep serving traffic.

That is what "active/active/active" means in this context. It does not mean one VIP is bound on three nodes at the same time. It means multiple VIPs are spread across three nodes so that all three nodes are active participants in normal operation.

## What keepAfloatD does

`keepAfloatD` is a standalone Linux daemon for VIP failover. Each instance reads one YAML configuration file that describes one Raft cluster, one health-check definition, and one shared VIP list. If you need multiple independent failover groups, you run multiple daemon instances with separate configs.

At a high level, each node does three things:

1. It runs a local health check.
2. It shares health information with the cluster through Raft.
3. It binds or unbinds VIPs locally with Linux networking commands such as `ip addr add` and `ip addr del`.

The important part is that VIP ownership is not decided by local guesswork. It is derived from committed cluster state. In other words, nodes do not just react to local health. They react to a consistent cluster-wide view of which members are healthy, fresh, and eligible to own VIPs.

When several VIPs are configured, `keepAfloatD` sorts them and distributes them round-robin across healthy nodes. That gives you deterministic placement and predictable rebalancing when the set of healthy nodes changes.

## How it differs from keepalived

`keepAfloatD` is not meant to replace every `keepalived` deployment. If you have a very simple active/passive setup and you are happy with VRRP-style failover, `keepalived` may still be enough.

The difference is the operational model. `keepalived` is commonly associated with leader/backup thinking. `keepAfloatD` is built around consensus-driven VIP assignment. It keeps the familiar idea of local health checks, but cluster decisions come from Raft, not from a standalone failover election per VIP.

That difference matters most when you want multi-active behavior. With `keepAfloatD`, multiple nodes can be active at the same time because several VIPs can be distributed across them. Ownership changes are also fenced: a new holder does not activate a VIP until the old holder has released it or has clearly become ineligible. That helps prevent address conflicts during failover.

So the most accurate way to compare the two is not "good versus bad." It is this: `keepAfloatD` is better suited for multi-active VIP failover scenarios.

## A simple three-node HAProxy example

Let us assume you have three Linux hosts:

- `node1`
- `node2`
- `node3`

Each node runs:

- HAProxy
- `keepAfloatD`

Now imagine you want to expose three virtual IPs:

- `192.0.2.101`
- `192.0.2.102`
- `192.0.2.103`

In a healthy three-node cluster, a simple distribution might look like this:

- `192.0.2.101` on `node1`
- `192.0.2.102` on `node2`
- `192.0.2.103` on `node3`

All three HAProxy nodes are now active. Clients can connect through any of those VIPs, and traffic is spread across the cluster through ownership of the VIPs themselves.

If `node2` becomes unhealthy, the cluster recalculates ownership from committed state. With two healthy nodes left, the same VIP set might rebalance like this:

- `192.0.2.101` on `node1`
- `192.0.2.102` on `node3`
- `192.0.2.103` on `node1`

The exact pattern is deterministic because both node IDs and VIPs are sorted before assignment. Every node that applies the same committed state reaches the same ownership result.

<!-- Image suggestion: before/after failover diagram showing VIP reassignment after one node goes unhealthy -->

## A configuration sketch

A `keepAfloatD` configuration is intentionally simple. One process equals one cluster, one health check, and one shared VIP list.

A stripped-down example looks like this:

```yaml
node_id: 1

raft_listen: "10.0.0.11:7000"
client_submit_listen: "10.0.0.11:7001"

peers:
  - id: 1
    raft_address: "10.0.0.11:7000"
    client_submit_address: "10.0.0.11:7001"
  - id: 2
    raft_address: "10.0.0.12:7000"
    client_submit_address: "10.0.0.12:7001"
  - id: 3
    raft_address: "10.0.0.13:7000"
    client_submit_address: "10.0.0.13:7001"

vips:
  - address: "192.0.2.101"
    interface: eth0
  - address: "192.0.2.102"
    interface: eth0
  - address: "192.0.2.103"
    interface: eth0

health:
  command: ["/bin/sh", "-c", "pgrep -x haproxy >/dev/null"]
  interval_ms: 2000
  timeout_ms: 3000
  stale_secs: 10

cluster_secret: "replace-me-with-a-random-secret"
```

All nodes must use the same peer list, the same VIP list, and the same health-check timing. The `node_id` and local listen addresses change per host, but the shared cluster view must remain identical.

For HAProxy, a process check like `pgrep -x haproxy` is a simple starting point. In production, you will usually want something stronger, such as a probe that confirms HAProxy is not only running but also ready to accept traffic.

## Deploying it on your hosts

The exact installation step depends on how you consume `keepAfloatD` in your environment, but the service layout is straightforward. The packaged `systemd` template runs the binary from `/usr/bin/keepafloatd` and reads `/etc/keepafloatd/config-%i.yaml`, while the default sample config lives at `/etc/keepafloatd/config.yaml` and can be copied to an instance-specific filename such as `/etc/keepafloatd/config-node1.yaml`.

A typical deployment flow is:

1. Install `keepAfloatD` on all three nodes.
2. Place the instance configuration file at `/etc/keepafloatd/config-node1.yaml` (adjust the instance name per host).
3. Adjust `node_id`, `raft_listen`, and `client_submit_listen` per host.
4. Keep the `peers`, `vips`, `health`, and `cluster_secret` values consistent across the cluster.
5. Enable and start the service.

In a systemd-based setup, that usually means:

```bash
cp /etc/keepafloatd/config.yaml /etc/keepafloatd/config-node1.yaml
systemctl enable --now keepafloatd@node1
```

`keepAfloatD` needs permission to add and remove VIPs on the host. In practice, that means `CAP_NET_ADMIN`, and sometimes `CAP_NET_RAW` if you use `arping`.

For first tests, you can also use dry-run mode and local examples to validate the cluster logic before touching real addresses.

## What happens during failure

This is where the consensus model becomes especially valuable.

Three independent conditions can force a node off a VIP:

- the local health check fails
- the node loses consensus freshness and can no longer commit updates
- committed ownership moves to another node

The first case is the easy one. If the local HAProxy health check starts failing, `keepAfloatD` marks the node unhealthy and unbinds the VIPs it currently holds.

The second case is more subtle and more important. If a node becomes isolated and can no longer successfully submit updates into the Raft cluster, it loses what `keepAfloatD` calls consensus freshness. In that state, it self-fences and gives up VIP ownership instead of continuing to hold a stale address.

The third case covers the cluster-wide ownership result. If committed state says a VIP belongs somewhere else, the old node must let it go. `keepAfloatD` also fences ownership changes with per-VIP generations, so a replacement node does not blindly bind a VIP while the previous owner may still be eligible.

That is a key difference between "I think the other node is gone" and "the cluster has safely moved ownership."

## Why this works well for HAProxy

HAProxy is a practical use case for `keepAfloatD` because the benefits are easy to see.

First, you use your infrastructure more efficiently. With several VIPs distributed across the cluster, all three HAProxy nodes can do useful work during normal operation.

Second, failover becomes easier to reason about. Ownership is derived from committed cluster state, not from loosely coupled local assumptions. When the healthy set changes, the VIP map changes deterministically on every node.

Third, you can scale horizontally in a way that feels natural. If you have more than one public entry point, you do not have to pin all of them to a single active node.

In short, `keepAfloatD` lets your HAProxy cluster behave more like a cluster and less like a single active appliance with warm spare boxes around it.

## Where keepAfloatD fits, and where it does not

`keepAfloatD` is a strong fit if you want:

- multi-active VIP distribution
- deterministic failover decisions
- several VIPs spread across several healthy Linux nodes
- a simple local health-check model with a stronger cluster coordination layer

It may be unnecessary if you only want:

- one VIP
- one simple active/passive pair
- classic VRRP behavior
- the smallest possible operational footprint

It is also worth being honest about the current boundaries of the project. In its current form, `keepAfloatD` is intentionally narrow: one daemon, one cluster, one health check, one shared VIP list. Membership is static in configuration, and the current implementation keeps Raft state in memory. That is a reasonable trade-off for a focused HA control plane, but it is still a trade-off.

## Final thoughts

If your current HAProxy failover model leaves most of your nodes waiting around for an outage, `keepAfloatD` is worth a closer look. It takes a familiar Linux VIP workflow and combines it with a Raft-based cluster model that is much better suited for multi-active operation.

The core idea is simple: health is local, ownership is committed, and VIP placement is deterministic. That gives you a practical way to run HAProxy in active/active/active mode across three nodes without pretending that all failover problems are just a matter of moving one VIP from one box to another.

If you want to try it yourself, start small: use three nodes, a few VIPs, and a health check that reflects whether HAProxy is truly ready to serve traffic. Once you see the reassignment behavior during a real failure test, the value of the model becomes very clear.
