# IDP Controller

A Kubernetes operator implementing a minimal Internal Developer Platform (IDP) on top of two custom resources — `Application` and `Database`. Each resource is backed by a controller with hand-written reconciliation logic, built directly on `sigs.k8s.io/controller-runtime` without relying on generated business logic.

The goal of this project is to let a developer describe a stateless service and its data dependency declaratively, and have the cluster derive every underlying Kubernetes object — Deployment, Service, Ingress, StatefulSet, PersistentVolumeClaim, and credential Secret — and keep them converged to that declaration.

```yaml
apiVersion: paas.internal/v1
kind: Database
metadata:
  name: postgres-test
spec:
  engine: postgres
  storage: 2Gi
---
apiVersion: paas.internal/v1
kind: Application
metadata:
  name: order-system-api
spec:
  image: ghcr.io/phumitada/order-system-api:b2caff3
  port: 5001
  imagePullSecret: ghcr-secret
  databaseRef:
    - postgres-test
  domain: order-system-api.example.com
```

Applying these two manifests provisions a Postgres StatefulSet with a generated credential Secret, and a Deployment/Service/Ingress for the application, automatically wired to consume that Secret as environment variables — with no manual Secret handling by the developer.

## Table of Contents

- [Architecture](#architecture)
- [Custom Resource Definitions](#custom-resource-definitions)
- [Controller Design](#controller-design)
  - [Application Controller](#application-controller)
  - [Database Controller](#database-controller)
- [Engineering Decisions](#engineering-decisions)
- [RBAC and Security Posture](#rbac-and-security-posture)
- [Getting Started](#getting-started)
- [Testing](#testing)
- [Known Limitations and Roadmap](#known-limitations-and-roadmap)
- [License](#license)

## Architecture

The controller manager runs two independent reconcilers registered against the `paas.internal/v1` API group.

```
                         ┌─────────────────────────────┐
                         │        kube-apiserver        │
                         └──────────────┬───────────────┘
                                         │ watch / list / patch
                    ┌────────────────────┴────────────────────┐
                    │                                          │
          ┌─────────▼─────────┐                     ┌──────────▼──────────┐
          │ DatabaseReconciler │                     │ ApplicationReconciler│
          └─────────┬─────────┘                     └──────────┬──────────┘
                     │ owns                                    │ owns
        ┌────────────┼────────────┐              ┌─────────────┼─────────────┐
        │            │            │              │             │             │
  ┌─────▼────┐ ┌─────▼─────┐ ┌────▼─────┐  ┌──────▼─────┐ ┌────▼────┐ ┌──────▼──────┐
  │ Secret   │ │StatefulSet│ │ Service  │  │ Deployment │ │ Service │ │  Ingress    │
  │(creds)   │ │           │ │(headless)│  │            │ │         │ │ (TLS)       │
  └────┬─────┘ └───────────┘ └──────────┘  └──────▲─────┘ └─────────┘ └─────────────┘
       │                                           │
       │            consumed as envFrom            │
       └───────────────────────────────────────────┘
                (cross-resource dependency, resolved by name)
```

`Database` and `Application` are independent resources — an `Application` references zero or more `Database` objects by name via `spec.databaseRef`, and the `ApplicationReconciler` resolves that reference at reconcile time by reading the `<database-name>-credentials` Secret that the `DatabaseReconciler` produces. There is no direct object ownership across the two CRDs; the coupling is intentionally loose and name-based, the same pattern used by most managed-database operators.

## Custom Resource Definitions

### `Database` (`paas.internal/v1`, Kind: `Database`)

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.engine` | `string` (`postgres` \| `redis`) | Yes | Selects the container image, listening port, and volume layout for the backing StatefulSet. |
| `spec.storage` | `resource.Quantity` | Yes | Requested capacity for the PersistentVolumeClaim. |
| `spec.name` | `*string` | No | Overrides the logical database name for Postgres (defaults to the resource name). Ignored for Redis. |

### `Application` (`paas.internal/v1`, Kind: `Application`)

| Field | Type | Required | Description |
|---|---|---|---|
| `spec.image` | `string` | Yes | Container image deployed to the Pod template. |
| `spec.port` | `int32` | Yes | Port exposed by the container, Service, and Ingress backend. |
| `spec.domain` | `*string` | No | If set, provisions a TLS-terminated Ingress on this host. |
| `spec.imagePullSecret` | `*string` | No | Image pull Secret name; defaults to `ghcr-secret`. |
| `spec.databaseRef` | `[]string` | No | Names of `Database` resources this Application consumes; each resolves to a `<name>-credentials` Secret injected via `envFrom`. |
| `spec.envSecretRefs` | `[]string` | No | Additional arbitrary Secrets injected via `envFrom`, independent of any `Database`. |

Both types carry a standard `status.conditions` block (`Available` / `Progressing` / `Degraded`), following upstream Kubernetes API conventions for observed state.

## Controller Design

### Application Controller

`internal/controller/application_controller.go`

The `Reconcile` function drives an `Application` toward three owned child resources — `Deployment`, `Service`, and (conditionally) `Ingress` — using a get-or-create, patch-on-drift loop against the API server, with each child resource carrying an owner reference back to the `Application` for garbage collection.

**Secret resolution.** Before building the Deployment, the reconciler resolves every entry in `spec.databaseRef` to a `<name>-credentials` Secret and every entry in `spec.envSecretRefs` to itself, and attaches each as an `envFrom.secretRef` source on the container. A missing Secret is treated as not-yet-ready rather than a hard failure — the reconciler skips it silently on `NotFound` and picks it up again once the Secret watch below fires, rather than blocking the entire reconcile on ordering between `Database` and `Application` creation.

**Deterministic rollout on Secret rotation.** Kubernetes Deployments do not restart Pods when a referenced Secret's contents change; the Pod template is byte-identical, so no new ReplicaSet is generated. This controller solves that by computing a SHA-256 checksum over the `resourceVersion` of every resolved Secret and stamping it onto the Pod template as a `checksum/secrets` annotation:

```go
sort.Strings(resourceVersions)
checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(resourceVersions, "-"))))
```

The `resourceVersions` slice is sorted before hashing. Kubernetes' List API does not guarantee stable ordering across calls, and without the sort, two reconciles over an unchanged set of Secrets could concatenate their resource versions in a different order and produce a different hash — forcing a spurious Pod rollout on every reconcile rather than only on genuine credential changes. Because the annotation lives on the Pod template (not just the Deployment), any change to it is picked up by the Deployment controller as a template diff and triggers a rolling update automatically.

**Reactive Secret watch.** The controller registers a `Watches` on `corev1.Secret` mapped through `findApplicationsForSecret`, which scans all `Application` objects and enqueues any whose `databaseRef` or `envSecretRefs` matches the changed Secret's name and namespace. This closes the loop end-to-end: a `Database` credential rotation, or an operator updating an application's environment Secret, results in an automatic, checksum-driven rollout of the consuming `Application` — without requiring the developer to touch the `Application` object or restart anything by hand.

**No plaintext credentials in the spec.** The `Application` CRD never carries a password, connection string, or other secret material directly — `databaseRef` is a name reference, and the actual credential exists only inside a `Secret` object created by the `Database` controller and mounted via `envFrom`. This keeps the CR itself safe to store in Git, log, or display in `kubectl get -o yaml` without leaking anything.

**Conditional Ingress with TLS.** When `spec.domain` is set, the controller provisions an `Ingress` with `cert-manager.io/cluster-issuer: letsencrypt-prod` and a `TLS` block pointing at a `<name>-tls` Secret, so certificate issuance is delegated entirely to cert-manager and requires no additional controller logic. When `spec.domain` is unset, the Ingress step is a no-op and any previously created Ingress is left untouched — the resource remains ClusterIP-only.

### Database Controller

`internal/controller/database_controller.go`

The `Reconcile` function drives a `Database` toward three owned child resources — a credential `Secret`, a `StatefulSet`, and a headless `Service` — in a fixed order: secret, then workload, then network identity, so the StatefulSet is never created before the credentials it mounts exist.

**Credential generation.** On first reconcile, `reconcileSecret` generates a 24-byte cryptographically secure random password using `crypto/rand` (not `math/rand`), base64-encodes it, and writes it into an engine-specific key set:

- `postgres`: `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, and a pre-assembled `DATABASE_URL`.
- `redis`: `REDIS_PASSWORD` and a pre-assembled `REDIS_URL`.

The Secret is created exactly once — if it already exists, `reconcileSecret` returns immediately without regenerating it — so a Database's password is stable for the resource's lifetime and safe against being wiped by an unrelated reconcile.

**Engine-parameterized workload.** `reconcileStatefulSet` selects the container image, listening port, volume name, and mount path from `spec.engine` (`postgres:15-alpine` on `5432` with a `pgdata` volume, or `redis:7-alpine` on `6379` with a `data` volume) and builds a single `StatefulSet` shape parameterized by those four values, rather than branching the entire object graph per engine. `spec.storage` is passed straight through to the `VolumeClaimTemplate`, so persistence is provisioned by the same StorageClass mechanism as any other stateful workload on the cluster.

**Headless networking.** The Service is created with `ClusterIP: None`, giving the StatefulSet stable per-Pod DNS identity (`<pod>.<service>.<namespace>.svc.cluster.local`) rather than a load-balanced virtual IP — the correct addressing mode for a singleton or clustered datastore where clients need to reach a specific member.

## Engineering Decisions

- **Idempotent, drift-correcting reconciles.** Every child-resource helper (`reconcileService`, `reconcileIngress`, `reconcileStatefulSet`, `reconcileSecret`) follows the same shape: `Get` the live object, `Create` on `NotFound`, `Update` on any other outcome, propagating the `resourceVersion` forward so the write is a valid optimistic-concurrency patch rather than a blind overwrite.
- **Owner references everywhere.** Every generated object carries a `metav1.OwnerReference` back to its parent CR, so deleting an `Application` or `Database` cascades through Kubernetes garbage collection instead of requiring explicit cleanup code in the controller.
- **Loose coupling between CRDs.** `Application` depends on `Database` purely by name, resolved at reconcile time through the Kubernetes API rather than a compile-time or webhook-enforced reference — an `Application` can be created before its `Database` exists and will self-heal once the Secret watch fires.
- **Least-privilege RBAC markers.** Controller-level `+kubebuilder:rbac` markers are scoped per resource and verb — for example, the Application controller only ever needs `get;list;watch` on Secrets (it never writes them), while the Database controller needs `create` on Secrets but not `update` or `delete`, since credentials are immutable after generation.

## RBAC and Security Posture

RBAC manifests are generated from source-level markers (`make manifests`) rather than hand-maintained, so the `ClusterRole` in `config/rbac/role.yaml` is guaranteed to match what the code actually calls. Per-resource `admin` / `editor` / `viewer` roles are also generated for both `Application` and `Database`, allowing namespace-level delegation of CR management without granting access to the underlying Secrets, Deployments, or StatefulSets directly.

## Getting Started

### Prerequisites

- Go 1.24+
- Docker 17.03+
- kubectl 1.11.3+
- Access to a Kubernetes 1.11.3+ cluster
- [cert-manager](https://cert-manager.io/) installed on the target cluster, with a `letsencrypt-prod` `ClusterIssuer`, if using `spec.domain` on `Application`

### Build and Deploy

```sh
# Build and push the manager image
make docker-build docker-push IMG=<registry>/idp-controller:tag

# Install the CRDs
make install

# Deploy the controller manager
make deploy IMG=<registry>/idp-controller:tag
```

### Try It

```sh
kubectl apply -f config/samples/test-database.yaml
kubectl apply -f config/samples/test-redis.yaml
kubectl apply -f config/samples/test-application.yaml
```

This provisions a Postgres and a Redis instance, then a Deployment/Service/Ingress consuming credentials from both, with an image pull Secret and a dedicated environment Secret attached.

### Uninstall

```sh
kubectl delete -k config/samples/
make uninstall
make undeploy
```

## Testing

```sh
make test        # unit + envtest suite (Ginkgo/Gomega, runs against a real API server via envtest)
make test-e2e     # end-to-end suite against a live cluster (e.g. Kind)
make lint         # golangci-lint
```

## Known Limitations and Roadmap

This is a learning-focused implementation of the operator pattern, and the following are known, intentional simplifications rather than oversights:

- Replica count is fixed at 1 for both `Deployment` and `StatefulSet`; `spec.replicas` is not yet exposed on either CRD.
- `Database` supports `postgres` and `redis` only; adding an engine means extending the parameter table in `reconcileStatefulSet`, not a structural change.
- No `status.conditions` are currently populated by either controller; the field exists on both types and is the natural next step for surfacing readiness to `kubectl get`.
- No finalizer-based cleanup is implemented — deletion relies entirely on Kubernetes owner-reference garbage collection, which is sufficient today since no child resource requires external (off-cluster) teardown.

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
