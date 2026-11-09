type: patch

### Fixed
- **The capacity check counts pipelines still starting** - two pipelines deployed to one runtime seconds apart both passed preflight while the first one's pods didn't exist yet, and one then waited forever on the namespace's ResourceQuota. Preflight now adds what the pipelines on that runtime without pods yet ask for, and names them.
- **A pod Kubernetes won't create says why** - the status said only "Waiting for the pod to be created" when a ResourceQuota (or an admission webhook) refused the pod; it now gives Kubernetes' reason.
