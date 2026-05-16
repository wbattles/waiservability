# Waiservability

Observability made simple.

Waiservability is a small Kubernetes observability UI.

## What it shows

- nodes: cpu, memory, uptime, pod load, pressure
- containers: grouped by namespace with cpu, memory, restarts, uptime, logs
- alerts
- events

## Sections

- **overview** — cluster totals and current alerts
- **nodes** — node health and resource usage
- **containers** — namespaces that open into pod details and logs
- **events** — recent cluster event feed

## Logs

Container logs stream live from the Kubernetes pod log API.

## Kubernetes

Run it inside Kubernetes.

Give it access to the Kubernetes API and metrics-server.

## What it needs

- access to read nodes
- access to read pods
- access to read services
- access to read events
- access to read pod logs
- access to read metrics from `metrics.k8s.io`

## Example RBAC

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: waiservability
  namespace: waisuite
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: waiservability
rules:
  - apiGroups: [""]
    resources: ["nodes", "pods", "services", "events", "pods/log"]
    verbs: ["get", "list"]
  - apiGroups: ["metrics.k8s.io"]
    resources: ["nodes", "pods"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: waiservability
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: waiservability
subjects:
  - kind: ServiceAccount
    name: waiservability
    namespace: waisuite
```

## Notes

- namespace usage is shown as share of cluster usage
- pod cpu and memory show percent of requested resources when requests exist
- if a pod has no request set, raw usage is shown instead

## License

MIT. See [LICENSE](LICENSE).
