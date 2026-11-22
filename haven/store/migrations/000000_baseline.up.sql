CREATE TABLE store_metadata (
	singleton INTEGER PRIMARY KEY CHECK(singleton=1),
	format TEXT NOT NULL,
	version INTEGER NOT NULL,
	identity TEXT NOT NULL,
	node_key_sha256 TEXT NOT NULL,
	epoch INTEGER NOT NULL CHECK(typeof(epoch)='integer' AND epoch>=0)
);

CREATE TABLE operations (
	id TEXT PRIMARY KEY NOT NULL CHECK(id<>''),
	scope_kind TEXT NOT NULL CHECK(scope_kind IN ('tenant','node')),
	scope_id TEXT NOT NULL CHECK(scope_id<>''),
	idem_key TEXT NOT NULL CHECK(idem_key<>''),
	request_hash TEXT NOT NULL,
	request BLOB NOT NULL,
	status TEXT NOT NULL CHECK(status IN ('pending','running','succeeded','failed')),
	body BLOB NOT NULL,
	created_at TEXT NOT NULL CHECK(created_at<>''),
	finished_at TEXT NOT NULL,
	CHECK((status IN ('succeeded','failed')) = (finished_at<>'')),
	UNIQUE(scope_kind,scope_id,idem_key)
);

CREATE TABLE operation_publications (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES operations(id),
	body BLOB NOT NULL
);

CREATE TABLE operation_steps (
	operation_id TEXT NOT NULL REFERENCES operations(id),
	step_key TEXT NOT NULL CHECK(step_key<>''),
	action TEXT NOT NULL CHECK(action<>''),
	input_hash TEXT NOT NULL,
	input BLOB NOT NULL,
	candidate_resource_id TEXT NOT NULL CHECK(candidate_resource_id<>''),
	expected_instance TEXT NOT NULL,
	status TEXT NOT NULL CHECK(status IN ('pending','running','succeeded','failed')),
	body BLOB NOT NULL,
	PRIMARY KEY(operation_id,step_key)
);

CREATE INDEX unfinished_steps_by_operation ON operation_steps(operation_id) WHERE status IN ('pending','running');

CREATE TABLE tenants (
	id TEXT PRIMARY KEY NOT NULL,
	token_hash TEXT NOT NULL UNIQUE,
	max_sandboxes INTEGER NOT NULL CHECK(typeof(max_sandboxes)='integer' AND max_sandboxes>=0),
	max_storage_mib INTEGER NOT NULL CHECK(typeof(max_storage_mib)='integer' AND max_storage_mib>=0)
);

CREATE TABLE sandboxes (
	id TEXT PRIMARY KEY NOT NULL,
	tenant TEXT NOT NULL,
	state TEXT NOT NULL,
	body BLOB NOT NULL
);

CREATE TABLE templates (
	id TEXT PRIMARY KEY NOT NULL,
	tenant TEXT NOT NULL,
	published_operation_id TEXT NOT NULL REFERENCES operations(id),
	body BLOB NOT NULL
);

CREATE TABLE template_owners (
	template_id TEXT PRIMARY KEY NOT NULL,
	tenant_id TEXT NOT NULL,
	candidate_operation_id TEXT NOT NULL REFERENCES operations(id)
);

CREATE TABLE snapshot_publications (
	snapshot_id TEXT PRIMARY KEY NOT NULL,
	tenant_id TEXT NOT NULL,
	body BLOB NOT NULL
);

CREATE TABLE checkpoint_bindings (
	id TEXT PRIMARY KEY NOT NULL,
	tenant TEXT NOT NULL,
	snapshot_id TEXT NOT NULL,
	operation_id TEXT NOT NULL REFERENCES operations(id),
	body BLOB NOT NULL
);

CREATE TABLE pool_slots (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES operations(id),
	template_id TEXT NOT NULL,
	template_version TEXT NOT NULL,
	vm_id TEXT NOT NULL UNIQUE,
	status TEXT NOT NULL CHECK(status IN ('pending','ready','selected','cleanup')),
	body BLOB NOT NULL
);

CREATE TABLE ready_warm_vms (
	vm_id TEXT PRIMARY KEY NOT NULL REFERENCES pool_slots(vm_id),
	template_id TEXT NOT NULL,
	snapshot_id TEXT NOT NULL,
	created_at_unix_ns INTEGER NOT NULL
);

CREATE INDEX ready_warm_selection ON ready_warm_vms(template_id,snapshot_id,created_at_unix_ns,vm_id);

CREATE TABLE snapshot_owners (
	snapshot_id TEXT PRIMARY KEY NOT NULL,
	tenant_id TEXT NOT NULL,
	source_operation_id TEXT NOT NULL REFERENCES operations(id),
	source_kind TEXT NOT NULL CHECK(source_kind IN ('capture','root_import'))
);

CREATE TABLE operation_request_macs (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES operations(id),
	request_mac TEXT NOT NULL
);

CREATE TABLE sandbox_settings (
	sandbox_id TEXT PRIMARY KEY NOT NULL REFERENCES sandboxes(id),
	body BLOB NOT NULL
);

CREATE TABLE network_policies (
	vm_id TEXT PRIMARY KEY NOT NULL REFERENCES vms(id),
	instance TEXT NOT NULL,
	operation_id TEXT NOT NULL REFERENCES operations(id),
	body BLOB NOT NULL
);

CREATE TABLE sandbox_operation_fences (
	sandbox_id TEXT PRIMARY KEY NOT NULL,
	operation_id TEXT NOT NULL REFERENCES operations(id)
);

CREATE TABLE vms (
	id TEXT PRIMARY KEY NOT NULL,
	slot INTEGER NOT NULL UNIQUE CHECK(slot>=0),
	current_instance TEXT NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('creating','created','running','stopped')),
	operation_id TEXT REFERENCES operations(id),
	body BLOB NOT NULL
);

CREATE TABLE instances (
	id TEXT PRIMARY KEY NOT NULL,
	vm_id TEXT NOT NULL REFERENCES vms(id),
	operation_id TEXT NOT NULL REFERENCES operations(id),
	body BLOB NOT NULL
);

CREATE TABLE resource_versions (
	operation_id TEXT PRIMARY KEY NOT NULL,
	vm_id TEXT NOT NULL REFERENCES vms(id),
	step_key TEXT NOT NULL,
	body BLOB NOT NULL,
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);

CREATE TABLE images (
	digest TEXT PRIMARY KEY NOT NULL,
	body BLOB NOT NULL
);

CREATE TABLE image_intents (
	digest TEXT PRIMARY KEY NOT NULL,
	operation_id TEXT NOT NULL UNIQUE REFERENCES operations(id),
	layout TEXT NOT NULL,
	body BLOB NOT NULL
);

CREATE TABLE image_caches (
	builder TEXT NOT NULL,
	diff_id TEXT NOT NULL,
	body BLOB NOT NULL,
	PRIMARY KEY(builder,diff_id)
);

CREATE TABLE snapshots (
	id TEXT PRIMARY KEY NOT NULL,
	source_vm_id TEXT NOT NULL,
	published INTEGER NOT NULL CHECK(published IN (0,1)),
	deleting INTEGER NOT NULL CHECK(deleting IN (0,1)),
	body BLOB NOT NULL
);

CREATE TABLE resource_refs (
	resource_kind TEXT NOT NULL,
	resource_id TEXT NOT NULL,
	holder_kind TEXT NOT NULL,
	holder_id TEXT NOT NULL,
	PRIMARY KEY(resource_kind,resource_id,holder_kind,holder_id)
);

CREATE TABLE snapshot_source_leases (
	snapshot_id TEXT NOT NULL REFERENCES snapshots(id),
	operation_id TEXT NOT NULL,
	step_key TEXT NOT NULL,
	PRIMARY KEY(operation_id,step_key),
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);

CREATE INDEX source_leases_by_snapshot ON snapshot_source_leases(snapshot_id);

CREATE TABLE vm_memory_backings (
	path TEXT PRIMARY KEY NOT NULL,
	vm_id TEXT NOT NULL REFERENCES vms(id),
	file_digest TEXT NOT NULL
);

CREATE TABLE host_capacity_reservations (
	owner_key TEXT PRIMARY KEY NOT NULL,
	storage_bytes INTEGER NOT NULL CHECK(typeof(storage_bytes)='integer' AND storage_bytes>=0),
	memory_mib INTEGER NOT NULL CHECK(typeof(memory_mib)='integer' AND memory_mib>=0),
	vcpus INTEGER NOT NULL CHECK(typeof(vcpus)='integer' AND vcpus>=0)
);

CREATE TABLE quota_resources (
	tenant_id TEXT NOT NULL,
	kind TEXT NOT NULL CHECK(kind IN ('sandbox_slot','build_slot','vm_disk','snapshot','pending_storage')),
	resource_key TEXT NOT NULL,
	sandbox_slots INTEGER NOT NULL CHECK(typeof(sandbox_slots)='integer' AND sandbox_slots>=0),
	storage_mib INTEGER NOT NULL CHECK(typeof(storage_mib)='integer' AND storage_mib>=0),
	source_operation_id TEXT NOT NULL REFERENCES operations(id),
	PRIMARY KEY(tenant_id,kind,resource_key)
);

CREATE TABLE quota_refs (
	tenant_id TEXT NOT NULL,
	kind TEXT NOT NULL,
	resource_key TEXT NOT NULL,
	holder_kind TEXT NOT NULL CHECK(holder_kind IN ('active_use','engine_dependency','cleanup')),
	holder_id TEXT NOT NULL,
	PRIMARY KEY(tenant_id,kind,resource_key,holder_kind,holder_id),
	FOREIGN KEY(tenant_id,kind,resource_key) REFERENCES quota_resources(tenant_id,kind,resource_key)
);

CREATE TABLE cleanup_intents (
	id TEXT PRIMARY KEY NOT NULL,
	operation_id TEXT NOT NULL REFERENCES operations(id),
	resource_kind TEXT NOT NULL,
	resource_id TEXT NOT NULL,
	status TEXT NOT NULL,
	body BLOB NOT NULL
);

CREATE UNIQUE INDEX active_image_collector ON cleanup_intents (
	resource_kind,
	resource_id
) WHERE resource_kind IN ('import_work','image_cache') AND status='pending';

CREATE UNIQUE INDEX active_supply_collector ON cleanup_intents (resource_id) WHERE resource_kind='supply_cleanup' AND status='pending';

CREATE TABLE deletion_intents (
	path TEXT PRIMARY KEY NOT NULL,
	operation_id TEXT NOT NULL,
	step_key TEXT NOT NULL,
	body BLOB NOT NULL,
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);

CREATE TABLE journal (
	seq INTEGER PRIMARY KEY AUTOINCREMENT,
	body BLOB NOT NULL,
	hash TEXT NOT NULL
);
