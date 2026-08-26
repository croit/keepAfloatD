# How keepAfloatD Can Bring Ceph RGW Gateways Online in a Multi-Active Cluster

Ceph object storage scales well on the backend, but many deployments still keep the frontend simple: a small number of ingress IPs, often tied to an active/passive failover model. That works, but it also means the traffic entry layer can remain narrower than the gateway layer behind it. If you already run five or seven RADOS Gateway nodes, it is fair to ask whether your ingress design is using them as effectively as it could.

That is where `keepAfloatD` becomes interesting. `keepAfloatD` does not replace Ceph, and it does not try to manage object placement, realms, zones, or RGW internals. Its job is much smaller and much more focused: it coordinates which Linux node should own which virtual IP right now, based on committed cluster state and local health.

For Ceph RGW, that gives you a practical way to move from "we have many gateways, but not all of them are truly active at the ingress layer" to "we have a multi-active VIP layout that keeps more gateways online and useful during normal operation."

In this post, we will look at how `keepAfloatD` can help expose Ceph RGW through a multi-active VIP design, why that can be attractive in clusters with five or seven gateways, and how to think about deployment in a way that stays honest about what `keepAfloatD` does and does not do.

<!-- Image suggestion: 5-node or 7-node Ceph RGW cluster with multiple VIPs distributed across healthy gateway nodes -->

## The problem is often not the gateways themselves

When teams think about Ceph RGW scaling, they naturally focus on the gateway nodes, the object workload, and the shape of client traffic. But the ingress layer is just as important. If all external traffic flows through a narrow active/passive entry point, the frontend design can become more conservative than the gateway layer behind it.

That can show up in a few ways:

- one node becomes the default owner of a public-facing VIP
- the rest of the gateway fleet is healthy, but not equally active from the client perspective
- maintenance and failover still work, but normal-state utilization is not ideal
- you have more gateway capacity than your ingress pattern really exposes

This is not a Ceph problem. It is a VIP ownership problem.

`keepAfloatD` addresses that problem by distributing several VIPs across several healthy nodes. Instead of treating the cluster as "one active ingress node plus spares," it treats the healthy set as a pool of eligible owners and assigns VIPs deterministically across it.

## What keepAfloatD actually brings to the table

`keepAfloatD` is a Linux daemon for VIP failover. It uses Raft to replicate health observations and ownership transitions, and then binds or unbinds VIPs locally with standard Linux networking tools.

That description matters because it keeps the scope clear. `keepAfloatD` is not:

- a replacement for Ceph monitor or manager logic
- a replacement for RGW itself
- an application-layer load balancer
- an S3-aware control plane

What it *is* is a cluster-local control plane for deciding who should own each virtual IP. That makes it a good fit for Ceph RGW because the gateway layer often already has multiple healthy nodes ready to serve traffic. The missing piece is coordinated, deterministic ingress ownership.

The model is intentionally small:

- one daemon process
- one Raft cluster
- one health-check definition
- one shared VIP list

If your environment needs isolated ingress groups, you run multiple instances with separate configuration.

## Why multi-active matters for RGW

In a Ceph RGW environment, the phrase "multi-active" can mean a few different things depending on context. Here we are talking specifically about the network entry layer, not about rewriting Ceph's internal architecture.

Multi-active in this case means:

- several VIPs exist
- several healthy RGW nodes own them at the same time
- several nodes are actively taking client traffic during normal operation

It does *not* mean one VIP is active on all gateway nodes at once.

This distinction is important because it keeps the design simple. With `keepAfloatD`, a VIP has one current holder. What changes is that you can have many VIPs distributed across many healthy gateways. That allows a five-node or seven-node RGW fleet to be more active from the ingress perspective instead of hiding behind one active holder and a set of backups.

## Why five or seven gateways are a natural fit

Martin mentioned examples with five or seven RGW gateways, and those numbers make sense for two reasons.

First, they are realistic sizes for object gateway fleets where teams want high availability without collapsing everything into one entry point.

Second, `keepAfloatD` uses Raft, which means quorum matters. Odd-sized clusters are attractive because they give you a clear majority decision model while making efficient use of nodes. In a five-node cluster, you can lose two voters and still keep a majority. In a seven-node cluster, you can lose three.

That does not mean you must always choose five or seven nodes. It means that if you already think in those sizes for RGW, the operational model lines up naturally with a consensus-based control plane.

## A practical example

Let us assume you have five RGW nodes:

- `rgw1`
- `rgw2`
- `rgw3`
- `rgw4`
- `rgw5`

Each node runs:

- `radosgw`
- `keepAfloatD`

Now imagine you expose five VIPs:

- `198.51.100.101`
- `198.51.100.102`
- `198.51.100.103`
- `198.51.100.104`
- `198.51.100.105`

In a healthy cluster, `keepAfloatD` can distribute those VIPs across the five healthy nodes. That means all five RGW nodes are actively participating at the ingress layer.

From there, you have options for how clients discover those VIPs. You might:

- publish several DNS records
- use a higher-level DNS or GSLB policy
- point an external load balancer at the VIP pool
- dedicate different VIPs to different client or application segments

The exact traffic pattern depends on your environment. The important point is that the RGW fleet is no longer hidden behind one active ingress owner.

## Health checks matter more than ever

For Ceph RGW, local health checks should be chosen carefully. A process-only check may be a useful smoke test, but it is often too weak to represent actual readiness for client traffic.

A better approach is usually to test something that reflects whether the local gateway can really serve requests. In practice, that might be:

- an HTTP check against a local RGW endpoint
- a small wrapper script that verifies local listener readiness
- a service-specific probe that reflects what "healthy enough to own a VIP" means in your environment

The key idea is simple: `keepAfloatD` will make good failover decisions only if the health signal you feed it is meaningful.

A very small example could look like this:

```yaml
health:
  command: ["/bin/sh", "-c", "curl -sf http://127.0.0.1:8080/ >/dev/null"]
  interval_ms: 2000
  timeout_ms: 3000
  stale_secs: 10
```

That exact URL is only a placeholder. In a real RGW deployment, you should point the probe at the local endpoint or wrapper that best reflects real gateway readiness.

## What happens when a gateway fails

This is where `keepAfloatD` becomes more than just "a thing that can move IPs."

If an RGW node becomes unhealthy, three protection mechanisms matter:

- the local health check fails
- the node can no longer keep consensus-fresh state in the Raft cluster
- committed VIP ownership is recalculated away from that node

The local health check is the most obvious trigger. If the local RGW health probe fails, `keepAfloatD` marks the node unhealthy and releases the VIPs it currently holds.

The second protection is more subtle. If a node is partitioned or otherwise loses the ability to submit updates through Raft, it also loses what `keepAfloatD` calls consensus freshness. In that state, it self-fences and unbinds VIPs instead of continuing to act like it is still a valid owner.

The third protection is cluster-wide. Once committed state says a VIP belongs elsewhere, the previous holder must step aside. Ownership transitions are also fenced so a replacement node does not bind too early while the previous holder may still be eligible.

This is especially useful in storage environments, where half-failed or partially isolated systems are often more dangerous than clean failures.

<!-- Image suggestion: one RGW node failing, its VIPs moving to the remaining healthy gateways -->

## Why this can be useful even if you already have load balancing

At first glance, some teams may ask a fair question: if I already have DNS or a load balancer, why do I need coordinated VIP ownership at all?

The answer depends on where you want high availability boundaries to live.

`keepAfloatD` can be useful when you want:

- highly available VIP ownership at the Linux host layer
- several stable ingress points instead of a single active one
- deterministic reassignment when a gateway fails
- a simple health-check driven control plane without introducing a full L7 dependency into the decision path

It does not replace every upstream traffic-management layer. Instead, it gives you a more resilient and more active set of ingress targets to put behind that layer.

For some environments, that is enough on its own. For others, it becomes a strong lower layer under DNS, anycast, or external load balancing.

## Where keepAfloatD fits in the Ceph story

It is worth being explicit here: `keepAfloatD` is not a "Ceph HA platform." It solves one narrow but important problem well.

It does not:

- replicate object data
- manage Ceph cluster membership
- understand bucket metadata
- replace RGW scaling strategy

It does:

- run a health check locally
- coordinate VIP ownership through Raft
- distribute multiple VIPs across healthy nodes
- remove failed or stale nodes from VIP eligibility

That narrow scope is a strength. It keeps the design understandable and gives you a very clear operational contract.

## When this approach is a good fit

`keepAfloatD` is a good fit for Ceph RGW ingress when:

- you have multiple gateway nodes
- you want more than one node active during normal operation
- you want deterministic VIP ownership
- you prefer simple Linux-level primitives and health checks
- you want failover decisions backed by consensus instead of loosely coupled local assumptions

It may be unnecessary when:

- one or two gateways are enough
- active/passive ingress already meets your needs
- your upstream traffic-management layer already solves the problem in a way you are fully happy with

As always, the right answer depends on your environment. The point is not that every RGW deployment must look like this. The point is that if you already have a larger gateway fleet, your ingress layer can become more capable too.

## Final thoughts

Ceph RGW clusters often have more frontend capacity available than their ingress design really exposes. `keepAfloatD` is a practical way to narrow that gap.

By distributing multiple VIPs across multiple healthy gateways, it lets more of your RGW fleet stay active during normal operation. When a node fails, the cluster reassigns ownership from committed state instead of relying on a narrow active/passive pattern. That does not replace the rest of your Ceph architecture, but it can make the entry layer much more aligned with the scale of the gateway layer behind it.

If you are running five or seven RGW gateways and you still expose them through a conservative ingress pattern, `keepAfloatD` is worth exploring. Start with a small lab, choose a health check that reflects true RGW readiness, and watch how the VIP map changes as healthy nodes come and go. Once you see the model in action, the advantage of a multi-active ingress layer becomes much easier to appreciate.
