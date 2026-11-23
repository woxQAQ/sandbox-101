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
	action TEXT NOT NULL CHECK(action<>''),
	request_kind TEXT NOT NULL CHECK(request_kind IN ('command','resource','resource_command','image_import','gc')),
	status TEXT NOT NULL CHECK(status IN ('pending','running','succeeded','failed')),
	result_sandbox_id TEXT NOT NULL DEFAULT '',
	result_vm_id TEXT NOT NULL DEFAULT '',
	result_image_digest TEXT NOT NULL DEFAULT '',
	result_template_id TEXT NOT NULL DEFAULT '',
	result_checkpoint_id TEXT NOT NULL DEFAULT '',
	result_snapshot_id TEXT NOT NULL DEFAULT '',
	resource_ready BOOLEAN NOT NULL DEFAULT 0 CHECK(resource_ready IN (0,1)),
	result_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL CHECK(created_at<>''),
	finished_at TEXT NOT NULL,
	CHECK((status IN ('succeeded','failed')) = (finished_at<>'')),
	UNIQUE(scope_kind,scope_id,idem_key)
);

CREATE TABLE operation_resource_requests (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES operations(id),
	template_id TEXT NOT NULL,
	sandbox_id TEXT NOT NULL,
	snapshot_id TEXT NOT NULL,
	expected_instance TEXT NOT NULL,
	stop_operation_id TEXT NOT NULL,
	source_operation_id TEXT NOT NULL,
	cache_builder TEXT NOT NULL,
	cache_diff_id TEXT NOT NULL
);
CREATE TABLE operation_import_requests (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES operations(id),
	layout TEXT NOT NULL,
	digest TEXT NOT NULL,
	materialization TEXT NOT NULL,
	max_bytes INTEGER NOT NULL,
	max_files INTEGER NOT NULL
);
CREATE TABLE operation_gc_requests (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES operations(id),
	dry_run BOOLEAN NOT NULL,
	batch INTEGER NOT NULL,
	budget_ns INTEGER NOT NULL
);
CREATE TABLE operation_commands (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES operations(id),
	claim_action TEXT,
	observed_generation TEXT NOT NULL,
	sandbox_id TEXT NOT NULL,
	checkpoint_id TEXT NOT NULL,
	template_id TEXT NOT NULL,
	ttl_seconds INTEGER NOT NULL,
	on_expire TEXT NOT NULL,
	auto_renew BOOLEAN NOT NULL,
	lease_deadline TEXT NOT NULL,
	policy_mode TEXT NOT NULL,
	max_connections INTEGER NOT NULL,
	branch_count INTEGER NOT NULL,
	has_template BOOLEAN NOT NULL,
	policy_rule_count INTEGER NOT NULL CHECK(policy_rule_count>=0),
	environment_count INTEGER NOT NULL CHECK(environment_count>=0)
);
CREATE TABLE command_templates (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES operation_commands(operation_id),
	id TEXT NOT NULL,
	tenant_id TEXT NOT NULL,
	image TEXT NOT NULL,
	vcpus INTEGER NOT NULL,
	memory_mib INTEGER NOT NULL,
	disk_mib INTEGER NOT NULL,
	snapshot_id TEXT NOT NULL,
	state TEXT NOT NULL,
	target_warm INTEGER NOT NULL,
	warmup TEXT NOT NULL
);
CREATE TABLE command_policy_rules (
	operation_id TEXT NOT NULL REFERENCES operation_commands(operation_id),
	kind TEXT NOT NULL CHECK(kind IN ('host','cidr')),
	position INTEGER NOT NULL CHECK(position>=0),
	value TEXT NOT NULL,
	PRIMARY KEY(operation_id,kind,position)
);
CREATE TABLE command_environment (
	operation_id TEXT NOT NULL REFERENCES operation_commands(operation_id),
	name TEXT NOT NULL,
	value TEXT NOT NULL,
	guest BOOLEAN,
	header TEXT,
	placeholder TEXT,
	host_count INTEGER CHECK(host_count IS NULL OR (host_count>=0 AND header IS NOT NULL)),
	CHECK((header IS NULL)=(placeholder IS NULL)),
	CHECK((guest IS NOT 0 AND header IS NULL) OR value=''),
	PRIMARY KEY(operation_id,name)
);
CREATE TABLE command_injection_hosts (
	operation_id TEXT NOT NULL,
	name TEXT NOT NULL,
	position INTEGER NOT NULL CHECK(position>=0),
	host TEXT NOT NULL,
	PRIMARY KEY(operation_id,name,position),
	FOREIGN KEY(operation_id,name) REFERENCES command_environment(operation_id,name)
);

CREATE TABLE operation_result_sandboxes (
	deleted_at TEXT,
	operation_id TEXT NOT NULL REFERENCES operations(id),
	position INTEGER NOT NULL,
	sandbox_id TEXT NOT NULL
);
CREATE UNIQUE INDEX current_result_positions ON operation_result_sandboxes(operation_id,position) WHERE deleted_at IS NULL;

CREATE TABLE operation_publications (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES operations(id),
	response_hash TEXT NOT NULL,
	charge_tenant_id TEXT NOT NULL,
	template_version_id TEXT REFERENCES template_versions(operation_id),
	template_state TEXT NOT NULL
);
CREATE TABLE published_sandboxes (
	operation_id TEXT NOT NULL REFERENCES operation_publications(operation_id),
	position INTEGER NOT NULL CHECK(position >= 0),
	sandbox_id TEXT NOT NULL,
	tenant_id TEXT NOT NULL,
	state TEXT NOT NULL,
	vm_id TEXT NOT NULL,
	snapshot_id TEXT NOT NULL,
	instance TEXT NOT NULL,
	template_id TEXT NOT NULL,
	provision_source TEXT NOT NULL,
	vcpus INTEGER NOT NULL,
	memory_mib INTEGER NOT NULL,
	disk_mib INTEGER NOT NULL,
	deadline TEXT NOT NULL,
	paused_until TEXT NOT NULL,
	ttl_seconds INTEGER NOT NULL,
	on_expire TEXT NOT NULL,
	auto_renew BOOLEAN NOT NULL,
	policy_mode TEXT NOT NULL,
	max_connections INTEGER NOT NULL,
	requires_secrets BOOLEAN NOT NULL,
	resume_attempt INTEGER NOT NULL,
	epoch TEXT NOT NULL,
	sandbox_operation_id TEXT NOT NULL,
	last_error TEXT NOT NULL,
	created_at TEXT NOT NULL,
	PRIMARY KEY(operation_id,position)
);
CREATE TABLE published_sandbox_rules (
	operation_id TEXT NOT NULL,
	sandbox_position INTEGER NOT NULL,
	kind TEXT NOT NULL CHECK(kind IN ('host','cidr')),
	position INTEGER NOT NULL CHECK(position >= 0),
	value TEXT NOT NULL,
	PRIMARY KEY(operation_id,sandbox_position,kind,position),
	FOREIGN KEY(operation_id,sandbox_position) REFERENCES published_sandboxes(operation_id,position)
);
CREATE TABLE published_checkpoints (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES operation_publications(operation_id),
	checkpoint_id TEXT NOT NULL,
	tenant_id TEXT NOT NULL,
	sandbox_id TEXT NOT NULL,
	snapshot_id TEXT NOT NULL,
	vcpus INTEGER NOT NULL,
	memory_mib INTEGER NOT NULL,
	disk_mib INTEGER NOT NULL,
	created_at TEXT NOT NULL
);

CREATE TABLE operation_steps (
	operation_id TEXT NOT NULL REFERENCES operations(id),
	step_key TEXT NOT NULL CHECK(step_key<>''),
	action TEXT NOT NULL CHECK(action<>''),
	input_hash TEXT NOT NULL,
	input BLOB,
	candidate_resource_id TEXT NOT NULL CHECK(candidate_resource_id<>''),
	expected_instance TEXT NOT NULL,
	status TEXT NOT NULL CHECK(status IN ('pending','running','succeeded','failed')),
	phase TEXT,
	completed_at TEXT NOT NULL DEFAULT '',
	result_snapshot_id TEXT NOT NULL DEFAULT '',
	receipts_hash TEXT,
	retired_by TEXT REFERENCES operations(id),
	step_error TEXT NOT NULL DEFAULT '',
	CHECK ((action='resource_link' AND input IS NULL AND phase IS NULL) OR
	       (step_key='warm_claim' AND action='claim' AND input IS NULL AND phase IS NOT NULL AND phase IN ('selected','warm_failed','warm_retired','complete','failed','superseded')) OR
	       (action='activate' AND input IS NOT NULL AND phase IS NOT NULL AND phase IN ('admitted','configuration_saved','off_intent','off','policy_intent','policy','activated','workload_ready','network_on','complete','blocked')) OR
	       (step_key IN ('image_import','image_cleanup','fork_group','capture','direct_retirement','gc','lifecycle','resource','supply','stop','snapshot_delete','capture_cleanup','capture_discard','supply_cleanup','discard') AND input IS NOT NULL AND phase IS NOT NULL AND phase<>'') OR
	       (step_key IN ('configuration','lease','checkpoint_delete') AND input IS NOT NULL AND phase IS NOT NULL AND phase='complete' AND status='succeeded')),
	PRIMARY KEY(operation_id,step_key)
);

CREATE INDEX unfinished_steps_by_operation ON operation_steps(operation_id) WHERE status IN ('pending','running');

CREATE TABLE image_import_progress (
	operation_id TEXT PRIMARY KEY NOT NULL,
	step_key TEXT NOT NULL DEFAULT 'image_import' CHECK(step_key='image_import'),
	failed_phase TEXT NOT NULL DEFAULT '',
	output_manifest_hash TEXT REFERENCES image_manifests(manifest_hash),
	output_file_hash TEXT,
	output_synced BOOLEAN,
	CHECK ((output_manifest_hash IS NULL)=(output_file_hash IS NULL) AND (output_manifest_hash IS NULL)=(output_synced IS NULL)),
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);
CREATE TABLE image_preparations (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES image_import_progress(operation_id),
	path TEXT NOT NULL,
	work_device INTEGER NOT NULL,
	work_inode INTEGER NOT NULL,
	work_mode INTEGER NOT NULL,
	output_device INTEGER NOT NULL,
	output_inode INTEGER NOT NULL,
	output_mode INTEGER NOT NULL
);
CREATE TABLE image_import_layers (
	operation_id TEXT NOT NULL REFERENCES image_import_progress(operation_id),
	ordinal INTEGER NOT NULL CHECK(ordinal>=0),
	tar_bytes INTEGER NOT NULL CHECK(tar_bytes>=0),
	entries INTEGER NOT NULL CHECK(entries>=0),
	cache_hash TEXT REFERENCES image_cache_witnesses(witness_hash),
	artifact_hash TEXT REFERENCES image_artifact_values(artifact_hash),
	PRIMARY KEY(operation_id,ordinal)
);

CREATE TABLE fork_progress (
	operation_id TEXT PRIMARY KEY NOT NULL,
	step_key TEXT NOT NULL DEFAULT 'fork_group' CHECK(step_key='fork_group'),
	capture_operation_id TEXT REFERENCES capture_progress(operation_id),
	publication_hash TEXT,
	child_count INTEGER NOT NULL DEFAULT 0 CHECK(child_count BETWEEN 0 AND 32),
	CHECK((capture_operation_id IS NULL)=(publication_hash IS NULL)),
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);

CREATE TABLE capture_progress (
	operation_id TEXT PRIMARY KEY NOT NULL,
	step_key TEXT NOT NULL DEFAULT 'capture' CHECK(step_key='capture'),
	resume_required BOOLEAN NOT NULL DEFAULT 0 CHECK(resume_required IN (0,1)),
	captured BOOLEAN NOT NULL DEFAULT 0 CHECK(captured IN (0,1)),
	materialized BOOLEAN NOT NULL DEFAULT 0 CHECK(materialized IN (0,1)),
	artifacts_synced BOOLEAN NOT NULL DEFAULT 0 CHECK(artifacts_synced IN (0,1)),
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);
CREATE TABLE capture_boundaries (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES capture_progress(operation_id),
	path TEXT NOT NULL,
	parent_device INTEGER NOT NULL,
	parent_inode INTEGER NOT NULL,
	parent_mode INTEGER NOT NULL,
	publication_device INTEGER NOT NULL,
	publication_inode INTEGER NOT NULL,
	publication_mode INTEGER NOT NULL
);
CREATE TABLE capture_files (
	operation_id TEXT NOT NULL REFERENCES capture_progress(operation_id),
	path TEXT NOT NULL,
	digest TEXT NOT NULL,
	PRIMARY KEY(operation_id,path)
);

CREATE TABLE gc_progress (
	operation_id TEXT PRIMARY KEY NOT NULL,
	step_key TEXT NOT NULL DEFAULT 'gc' CHECK(step_key='gc'),
	next_candidate INTEGER NOT NULL DEFAULT 0 CHECK(next_candidate>=0),
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);
CREATE TABLE gc_outcomes (
	operation_id TEXT NOT NULL REFERENCES gc_progress(operation_id),
	disposition TEXT NOT NULL CHECK(disposition IN ('collected','deferred')),
	ordinal INTEGER NOT NULL CHECK(ordinal>=0),
	kind TEXT NOT NULL,
	resource_id TEXT NOT NULL,
	tenant_id TEXT NOT NULL,
	cache_builder TEXT NOT NULL,
	cache_diff_id TEXT NOT NULL,
	proof TEXT NOT NULL,
	child_operation_id TEXT NOT NULL,
	reason TEXT NOT NULL,
	PRIMARY KEY(operation_id,disposition,ordinal)
);

CREATE TABLE resource_progress (
	operation_id TEXT PRIMARY KEY NOT NULL,
	step_key TEXT NOT NULL DEFAULT 'resource' CHECK(step_key='resource'),
	restore_snapshot_id TEXT,
	backing_holder TEXT,
	backing_ready BOOLEAN NOT NULL DEFAULT 0 CHECK(backing_ready IN (0,1)),
	restore_blocked BOOLEAN NOT NULL DEFAULT 0 CHECK(restore_blocked IN (0,1)),
	disk_device INTEGER,
	disk_inode INTEGER,
	disk_mode INTEGER,
	materials_hash TEXT,
	observation_present BOOLEAN NOT NULL DEFAULT 0,
	alive BOOLEAN NOT NULL DEFAULT 0,
	paused BOOLEAN NOT NULL DEFAULT 0,
	booted BOOLEAN NOT NULL DEFAULT 0,
	configured BOOLEAN NOT NULL DEFAULT 0,
	pid INTEGER NOT NULL DEFAULT 0,
	start_time INTEGER NOT NULL DEFAULT 0,
	network BOOLEAN NOT NULL DEFAULT 0,
	result_instance TEXT NOT NULL DEFAULT '',
	error TEXT NOT NULL DEFAULT '',
	CHECK((disk_device IS NULL)=(disk_inode IS NULL) AND (disk_device IS NULL)=(disk_mode IS NULL)),
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);
CREATE TABLE restore_materials (
	deleted_at TEXT,
	operation_id TEXT NOT NULL REFERENCES resource_progress(operation_id),
	active_operation_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN operation_id END) UNIQUE,
	complete BOOLEAN NOT NULL,
	files_present BOOLEAN NOT NULL,
	version_device INTEGER NOT NULL,
	version_inode INTEGER NOT NULL,
	version_mode INTEGER NOT NULL,
	restore_device INTEGER NOT NULL,
	restore_inode INTEGER NOT NULL,
	restore_mode INTEGER NOT NULL
);
CREATE INDEX current_restore_materials ON restore_materials(operation_id) WHERE deleted_at IS NULL;
CREATE TABLE restore_files (
	deleted_at TEXT,
	operation_id TEXT NOT NULL,
	active_operation_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN operation_id END) REFERENCES restore_materials(active_operation_id),
	name TEXT NOT NULL,
	source TEXT NOT NULL,
	candidate TEXT NOT NULL,
	size INTEGER NOT NULL,
	device INTEGER NOT NULL,
	inode INTEGER NOT NULL,
	mode INTEGER NOT NULL
);
CREATE UNIQUE INDEX current_restore_files ON restore_files(operation_id,name) WHERE deleted_at IS NULL;
CREATE INDEX resource_backing_sources ON resource_progress(restore_snapshot_id,backing_holder) WHERE backing_holder IS NOT NULL;

CREATE TABLE resource_links (
	parent_operation_id TEXT NOT NULL,
	step_key TEXT NOT NULL CHECK(step_key LIKE 'link:%'),
	child_operation_id TEXT NOT NULL REFERENCES operations(id) DEFERRABLE INITIALLY DEFERRED,
	child_kind TEXT NOT NULL CHECK(child_kind<>''),
	PRIMARY KEY(parent_operation_id,step_key),
	FOREIGN KEY(parent_operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);
CREATE INDEX resource_links_by_child ON resource_links(child_operation_id,step_key);

CREATE TABLE warm_claims (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES operations(id),
	step_key TEXT NOT NULL DEFAULT 'warm_claim' CHECK(step_key='warm_claim'),
	refill_operation_id TEXT NOT NULL UNIQUE REFERENCES operations(id),
	source_operation_id TEXT NOT NULL REFERENCES resource_progress(operation_id),
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);

CREATE TABLE tenants (
	id TEXT PRIMARY KEY NOT NULL,
	token_hash TEXT NOT NULL UNIQUE,
	max_sandboxes INTEGER NOT NULL CHECK(typeof(max_sandboxes)='integer' AND max_sandboxes>=0),
	max_storage_mib INTEGER NOT NULL CHECK(typeof(max_storage_mib)='integer' AND max_storage_mib>=0)
);

CREATE TABLE sandboxes (
	id TEXT PRIMARY KEY NOT NULL,
	tenant TEXT NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('provisioning','running','paused','terminated')),
	template_id TEXT NOT NULL,
	vcpus INTEGER NOT NULL,
	memory_mib INTEGER NOT NULL,
	disk_mib INTEGER NOT NULL,
	creation_epoch TEXT NOT NULL,
	created_at TEXT NOT NULL,
	provision_source TEXT NOT NULL,
	last_error TEXT NOT NULL,
	operation_id TEXT REFERENCES operations(id) DEFERRABLE INITIALLY DEFERRED,
	vm_id TEXT NOT NULL,
	instance TEXT NOT NULL,
	snapshot_id TEXT NOT NULL,
	source_operation_id TEXT REFERENCES operations(id) DEFERRABLE INITIALLY DEFERRED,
	slot_operation_id TEXT REFERENCES operations(id) DEFERRABLE INITIALLY DEFERRED,
	slot_input_hash TEXT,
	CHECK((slot_operation_id IS NULL)=(slot_input_hash IS NULL))
);

CREATE TABLE template_versions (
	operation_id TEXT PRIMARY KEY NOT NULL REFERENCES operations(id),
	template_id TEXT NOT NULL REFERENCES template_owners(template_id),
	image TEXT NOT NULL,
	vcpus INTEGER NOT NULL CHECK(vcpus>0),
	memory_mib INTEGER NOT NULL CHECK(memory_mib>0),
	disk_mib INTEGER NOT NULL CHECK(disk_mib>0),
	warmup TEXT NOT NULL,
	snapshot_id TEXT NOT NULL,
	target_warm INTEGER NOT NULL CHECK(target_warm>=0),
	UNIQUE(template_id,operation_id)
);
CREATE INDEX template_versions_by_snapshot ON template_versions(snapshot_id);
CREATE TABLE template_version_labels (
	version_id TEXT NOT NULL REFERENCES template_versions(operation_id),
	name TEXT NOT NULL,
	value TEXT NOT NULL,
	PRIMARY KEY(version_id,name)
);
CREATE TABLE templates (
	id TEXT PRIMARY KEY NOT NULL,
	published_operation_id TEXT NOT NULL,
	FOREIGN KEY(id,published_operation_id) REFERENCES template_versions(template_id,operation_id)
);

CREATE TABLE template_owners (
	template_id TEXT PRIMARY KEY NOT NULL,
	tenant_id TEXT NOT NULL,
	candidate_operation_id TEXT NOT NULL REFERENCES operations(id)
);

CREATE TABLE snapshot_publications (
	deleted_at TEXT,
	snapshot_id TEXT NOT NULL,
	active_snapshot_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN snapshot_id END) UNIQUE REFERENCES snapshots(active_id),
	tenant_id TEXT NOT NULL,
	capture_operation_id TEXT NOT NULL REFERENCES operations(id),
	record_hash TEXT NOT NULL
);

CREATE INDEX current_snapshot_publications ON snapshot_publications(snapshot_id) WHERE deleted_at IS NULL;

CREATE TABLE checkpoint_bindings (
	deleted_at TEXT,
	id TEXT NOT NULL,
	tenant TEXT NOT NULL,
	snapshot_id TEXT NOT NULL,
	operation_id TEXT NOT NULL REFERENCES operations(id),
	sandbox_id TEXT NOT NULL REFERENCES sandboxes(id),
	template_id TEXT NOT NULL,
	vcpus INTEGER NOT NULL,
	memory_mib INTEGER NOT NULL,
	disk_mib INTEGER NOT NULL,
	created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX current_checkpoint_ids ON checkpoint_bindings(id) WHERE deleted_at IS NULL;

CREATE TABLE pool_slots (
	deleted_at TEXT,
	operation_id TEXT NOT NULL REFERENCES operations(id),
	template_id TEXT NOT NULL,
	template_version TEXT NOT NULL,
	sandbox_id TEXT NOT NULL,
	vm_id TEXT NOT NULL,
	source_operation_id TEXT NOT NULL REFERENCES operations(id) DEFERRABLE INITIALLY DEFERRED,
	source_input_hash TEXT NOT NULL,
	instance TEXT NOT NULL DEFAULT '',
	created_at_unix_ns INTEGER NOT NULL,
	status TEXT NOT NULL CHECK(status IN ('pending','ready','selected','cleanup')),
	CHECK(status NOT IN ('ready','selected') OR instance<>''),
	FOREIGN KEY(template_id,template_version) REFERENCES template_versions(template_id,operation_id)
);
CREATE UNIQUE INDEX current_pool_operations ON pool_slots(operation_id) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX current_pool_vms ON pool_slots(vm_id) WHERE deleted_at IS NULL;
CREATE INDEX ready_warm_selection ON pool_slots(template_id,template_version,created_at_unix_ns,vm_id) WHERE deleted_at IS NULL AND status='ready';

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
	deleted_at TEXT,
	sandbox_id TEXT NOT NULL REFERENCES sandboxes(id),
	active_sandbox_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN sandbox_id END) UNIQUE,
	generation TEXT NOT NULL REFERENCES operations(id) DEFERRABLE INITIALLY DEFERRED,
	instance TEXT NOT NULL,
	active_epoch TEXT NOT NULL,
	ttl_seconds INTEGER NOT NULL,
	on_expire TEXT NOT NULL,
	auto_renew BOOLEAN NOT NULL,
	deadline TEXT NOT NULL,
	paused_until TEXT NOT NULL,
	policy_mode TEXT NOT NULL,
	max_connections INTEGER NOT NULL,
	env_present BOOLEAN NOT NULL
);
CREATE INDEX current_sandbox_settings ON sandbox_settings(sandbox_id) WHERE deleted_at IS NULL;
CREATE TABLE sandbox_environment (
	deleted_at TEXT,
	sandbox_id TEXT NOT NULL,
	active_sandbox_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN sandbox_id END) REFERENCES sandbox_settings(active_sandbox_id),
	name TEXT NOT NULL,
	value TEXT NOT NULL
);
CREATE UNIQUE INDEX current_environment_names ON sandbox_environment(sandbox_id,name) WHERE deleted_at IS NULL;
CREATE TABLE sandbox_policy_rules (
	deleted_at TEXT,
	sandbox_id TEXT NOT NULL,
	active_sandbox_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN sandbox_id END) REFERENCES sandbox_settings(active_sandbox_id),
	kind TEXT NOT NULL CHECK(kind IN ('host','cidr')),
	position INTEGER NOT NULL CHECK(position>=0),
	value TEXT NOT NULL
);
CREATE UNIQUE INDEX current_policy_positions ON sandbox_policy_rules(sandbox_id,kind,position) WHERE deleted_at IS NULL;
CREATE TABLE sandbox_secret_descriptors (
	deleted_at TEXT,
	sandbox_id TEXT NOT NULL,
	active_sandbox_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN sandbox_id END) REFERENCES sandbox_settings(active_sandbox_id),
	name TEXT NOT NULL,
	header TEXT NOT NULL,
	placeholder TEXT NOT NULL,
	UNIQUE(active_sandbox_id,name)
);
CREATE INDEX current_secret_descriptors ON sandbox_secret_descriptors(sandbox_id,name) WHERE deleted_at IS NULL;
CREATE TABLE sandbox_secret_hosts (
	deleted_at TEXT,
	active_sandbox_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN sandbox_id END),
	sandbox_id TEXT NOT NULL,
	name TEXT NOT NULL,
	position INTEGER NOT NULL CHECK(position>=0),
	host TEXT NOT NULL,
	FOREIGN KEY(active_sandbox_id,name) REFERENCES sandbox_secret_descriptors(active_sandbox_id,name)
);
CREATE UNIQUE INDEX current_secret_host_positions ON sandbox_secret_hosts(sandbox_id,name,position) WHERE deleted_at IS NULL;

CREATE TABLE network_policies (
	deleted_at TEXT,
	vm_id TEXT NOT NULL,
	active_vm_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN vm_id END) UNIQUE REFERENCES vms(active_id),
	instance TEXT NOT NULL,
	operation_id TEXT NOT NULL REFERENCES operations(id),
	policy_hash TEXT,
	retirement_step TEXT CHECK(retirement_step='direct_retirement'),
	CHECK((policy_hash IS NULL)=(retirement_step IS NULL)),
	FOREIGN KEY(operation_id,retirement_step) REFERENCES operation_steps(operation_id,step_key)
);

CREATE INDEX current_network_policies ON network_policies(vm_id) WHERE deleted_at IS NULL;

CREATE TABLE sandbox_operation_fences (
	deleted_at TEXT,
	sandbox_id TEXT NOT NULL,
	operation_id TEXT NOT NULL REFERENCES operations(id)
);

CREATE UNIQUE INDEX current_sandbox_fences ON sandbox_operation_fences(sandbox_id) WHERE deleted_at IS NULL;

CREATE TABLE vms (
	deleted_at TEXT,
	id TEXT NOT NULL,
	active_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN id END) UNIQUE,
	slot INTEGER NOT NULL CHECK(slot>=0),
	current_instance TEXT NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('creating','created','running','stopped')),
	operation_id TEXT REFERENCES operations(id),
	generation_operation_id TEXT NOT NULL REFERENCES operations(id),
	directory TEXT NOT NULL,
	resource_hash TEXT NOT NULL,
	verified INTEGER NOT NULL DEFAULT 0 CHECK(verified IN (0,1)),
	agent_ready INTEGER NOT NULL DEFAULT 0 CHECK(agent_ready IN (0,1)),
	deleting INTEGER NOT NULL DEFAULT 0 CHECK(deleting IN (0,1)),
	observation_hash TEXT
);

CREATE INDEX current_vm_ids ON vms(id) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX current_vm_slots ON vms(slot) WHERE deleted_at IS NULL;

CREATE TABLE instances (
	deleted_at TEXT,
	id TEXT NOT NULL,
	vm_id TEXT NOT NULL,
	active_vm_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN vm_id END) REFERENCES vms(active_id),
	operation_id TEXT NOT NULL REFERENCES operations(id),
	resource_hash TEXT NOT NULL,
	observation_hash TEXT
);

CREATE UNIQUE INDEX current_instance_ids ON instances(id) WHERE deleted_at IS NULL;

CREATE TABLE resource_versions (
	deleted_at TEXT,
	operation_id TEXT NOT NULL,
	vm_id TEXT NOT NULL,
	active_vm_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN vm_id END) REFERENCES vms(active_id),
	step_key TEXT NOT NULL,
	instance TEXT NOT NULL CHECK(instance<>''),
	resource_hash TEXT NOT NULL,
	disk_device INTEGER,
	disk_inode INTEGER,
	disk_mode INTEGER,
	CHECK((disk_device IS NULL)=(disk_inode IS NULL) AND (disk_device IS NULL)=(disk_mode IS NULL)),
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);

CREATE UNIQUE INDEX current_resource_versions ON resource_versions(operation_id) WHERE deleted_at IS NULL;

CREATE TABLE image_manifests (
	manifest_hash TEXT PRIMARY KEY NOT NULL,
	digest TEXT NOT NULL,
	format INTEGER NOT NULL,
	layout TEXT NOT NULL,
	architecture TEXT NOT NULL,
	builder TEXT NOT NULL,
	layers_present BOOLEAN NOT NULL,
	tools_present BOOLEAN NOT NULL,
	UNIQUE(digest,manifest_hash)
);
CREATE TABLE images (
	digest TEXT PRIMARY KEY NOT NULL,
	manifest_hash TEXT NOT NULL,
	FOREIGN KEY(digest,manifest_hash) REFERENCES image_manifests(digest,manifest_hash)
);
CREATE TABLE image_layers (
	manifest_hash TEXT NOT NULL REFERENCES image_manifests(manifest_hash),
	position INTEGER NOT NULL,
	layer_digest TEXT NOT NULL,
	diff_id TEXT NOT NULL,
	PRIMARY KEY(manifest_hash,position)
);
CREATE INDEX image_layers_by_diff ON image_layers(diff_id,manifest_hash);
CREATE TABLE image_tools (
	manifest_hash TEXT NOT NULL REFERENCES image_manifests(manifest_hash),
	name TEXT NOT NULL,
	version TEXT NOT NULL,
	PRIMARY KEY(manifest_hash,name)
);
CREATE TABLE image_files (
	manifest_hash TEXT NOT NULL REFERENCES image_manifests(manifest_hash),
	path TEXT NOT NULL,
	sha256 TEXT NOT NULL,
	PRIMARY KEY(manifest_hash,path)
);
CREATE TABLE image_artifact_values (
	artifact_hash TEXT PRIMARY KEY NOT NULL,
	role TEXT NOT NULL,
	path TEXT NOT NULL,
	sha256 TEXT NOT NULL,
	diff_id TEXT NOT NULL,
	device_id INTEGER NOT NULL,
	guest_path TEXT NOT NULL,
	serial TEXT NOT NULL,
	cache_sha256 TEXT NOT NULL
);
CREATE TABLE image_artifacts (
	manifest_hash TEXT NOT NULL REFERENCES image_manifests(manifest_hash),
	position INTEGER NOT NULL,
	artifact_hash TEXT NOT NULL REFERENCES image_artifact_values(artifact_hash),
	PRIMARY KEY(manifest_hash,position)
);

CREATE TABLE image_intents (
	deleted_at TEXT,
	digest TEXT NOT NULL,
	operation_id TEXT NOT NULL REFERENCES operations(id),
	layout TEXT NOT NULL,
	step_key TEXT NOT NULL DEFAULT 'image_import' CHECK(step_key='image_import'),
	input_hash TEXT NOT NULL,
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);

CREATE UNIQUE INDEX current_image_intent_digest ON image_intents(digest) WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX current_image_intent_operation ON image_intents(operation_id) WHERE deleted_at IS NULL;

CREATE TABLE image_cache_witnesses (
	witness_hash TEXT PRIMARY KEY NOT NULL,
	builder TEXT NOT NULL,
	diff_id TEXT NOT NULL,
	physical_bytes INTEGER NOT NULL CHECK(physical_bytes>0),
	path TEXT NOT NULL,
	directory_device INTEGER NOT NULL,
	directory_inode INTEGER NOT NULL,
	directory_mode INTEGER NOT NULL,
	data_device INTEGER NOT NULL,
	data_inode INTEGER NOT NULL,
	data_mode INTEGER NOT NULL,
	receipt_device INTEGER NOT NULL,
	receipt_inode INTEGER NOT NULL,
	receipt_mode INTEGER NOT NULL,
	receipt_sha256 TEXT NOT NULL,
	format INTEGER NOT NULL,
	profile TEXT NOT NULL,
	sha256 TEXT NOT NULL,
	tar_bytes INTEGER NOT NULL,
	entries INTEGER NOT NULL,
	UNIQUE(builder,diff_id,witness_hash)
);
CREATE TABLE image_caches (
	deleted_at TEXT,
	builder TEXT NOT NULL,
	diff_id TEXT NOT NULL,
	witness_hash TEXT NOT NULL,
	source_operation_id TEXT NOT NULL REFERENCES operations(id),
	FOREIGN KEY(builder,diff_id,witness_hash) REFERENCES image_cache_witnesses(builder,diff_id,witness_hash)
);
CREATE UNIQUE INDEX current_image_caches ON image_caches(builder,diff_id) WHERE deleted_at IS NULL;
CREATE INDEX image_caches_by_source ON image_caches(source_operation_id) WHERE deleted_at IS NULL;

CREATE TABLE snapshots (
	deleted_at TEXT,
	id TEXT NOT NULL,
	active_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN id END) UNIQUE,
	source_vm_id TEXT NOT NULL,
	tenant_id TEXT NOT NULL,
	capture_operation_id TEXT NOT NULL REFERENCES operations(id),
	source_instance TEXT NOT NULL,
	directory TEXT NOT NULL,
	published INTEGER NOT NULL CHECK(published IN (0,1)),
	deleting INTEGER NOT NULL CHECK(deleting IN (0,1)),
	manifest_hash TEXT,
	CHECK((published=1)=(manifest_hash IS NOT NULL))
);

CREATE INDEX current_snapshot_ids ON snapshots(id) WHERE deleted_at IS NULL;

CREATE TABLE resource_refs (
	deleted_at TEXT,
	resource_kind TEXT NOT NULL,
	resource_id TEXT NOT NULL,
	holder_kind TEXT NOT NULL,
	holder_id TEXT NOT NULL
);
CREATE UNIQUE INDEX current_resource_refs ON resource_refs(resource_kind,resource_id,holder_kind,holder_id) WHERE deleted_at IS NULL;

CREATE TABLE snapshot_source_leases (
	deleted_at TEXT,
	snapshot_id TEXT NOT NULL,
	active_snapshot_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN snapshot_id END) REFERENCES snapshots(active_id),
	operation_id TEXT NOT NULL,
	step_key TEXT NOT NULL,
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);
CREATE UNIQUE INDEX current_snapshot_source_steps ON snapshot_source_leases(operation_id,step_key) WHERE deleted_at IS NULL;

CREATE INDEX source_leases_by_snapshot ON snapshot_source_leases(snapshot_id) WHERE deleted_at IS NULL;

CREATE TABLE vm_memory_backings (
	deleted_at TEXT,
	path TEXT NOT NULL,
	vm_id TEXT NOT NULL,
	active_vm_id TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN vm_id END) REFERENCES vms(active_id),
	file_digest TEXT NOT NULL
);

CREATE UNIQUE INDEX current_memory_backings ON vm_memory_backings(path) WHERE deleted_at IS NULL;

CREATE TABLE host_capacity_reservations (
	deleted_at TEXT,
	owner_key TEXT NOT NULL,
	storage_bytes INTEGER NOT NULL CHECK(typeof(storage_bytes)='integer' AND storage_bytes>=0),
	memory_mib INTEGER NOT NULL CHECK(typeof(memory_mib)='integer' AND memory_mib>=0),
	vcpus INTEGER NOT NULL CHECK(typeof(vcpus)='integer' AND vcpus>=0)
);

CREATE UNIQUE INDEX current_capacity_owners ON host_capacity_reservations(owner_key) WHERE deleted_at IS NULL;

CREATE TABLE quota_resources (
	deleted_at TEXT,
	active_resource_key TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN resource_key END),
	tenant_id TEXT NOT NULL,
	kind TEXT NOT NULL CHECK(kind IN ('sandbox_slot','build_slot','vm_disk','snapshot','pending_storage')),
	resource_key TEXT NOT NULL,
	sandbox_slots INTEGER NOT NULL CHECK(typeof(sandbox_slots)='integer' AND sandbox_slots>=0),
	storage_mib INTEGER NOT NULL CHECK(typeof(storage_mib)='integer' AND storage_mib>=0),
	source_operation_id TEXT NOT NULL REFERENCES operations(id),
	UNIQUE(tenant_id,kind,active_resource_key)
);

CREATE INDEX current_quota_costs ON quota_resources(tenant_id,kind,resource_key) WHERE deleted_at IS NULL;

CREATE TABLE quota_refs (
	deleted_at TEXT,
	active_resource_key TEXT GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN resource_key END),
	tenant_id TEXT NOT NULL,
	kind TEXT NOT NULL,
	resource_key TEXT NOT NULL,
	holder_kind TEXT NOT NULL CHECK(holder_kind IN ('active_use','engine_dependency','cleanup')),
	holder_id TEXT NOT NULL,
	FOREIGN KEY(tenant_id,kind,active_resource_key) REFERENCES quota_resources(tenant_id,kind,active_resource_key)
);
CREATE UNIQUE INDEX current_quota_refs ON quota_refs(tenant_id,kind,resource_key,holder_kind,holder_id) WHERE deleted_at IS NULL;

CREATE TABLE cleanup_intents (
	id TEXT PRIMARY KEY NOT NULL,
	operation_id TEXT NOT NULL REFERENCES operations(id),
	resource_kind TEXT NOT NULL,
	resource_id TEXT NOT NULL,
	status TEXT NOT NULL CHECK(status IN ('armed','blocked','pending','retained','transferred','done')),
	step_key TEXT NOT NULL,
	witness_hash TEXT NOT NULL,
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key) DEFERRABLE INITIALLY DEFERRED
);

CREATE UNIQUE INDEX active_image_collector ON cleanup_intents (
	resource_kind,
	resource_id
) WHERE resource_kind IN ('import_work','image_cache') AND status='pending';

CREATE UNIQUE INDEX active_supply_collector ON cleanup_intents (resource_id) WHERE resource_kind='supply_cleanup' AND status='pending';

CREATE TABLE deletion_history (
	path TEXT NOT NULL,
	operation_id TEXT NOT NULL,
	step_key TEXT NOT NULL,
	format INTEGER NOT NULL CHECK(format=1),
	directory BOOLEAN NOT NULL CHECK(directory IN (0,1)),
	receipt_hash TEXT NOT NULL,
	ordinal INTEGER NOT NULL CHECK(ordinal>=0),
	UNIQUE(operation_id,step_key,ordinal),
	PRIMARY KEY(operation_id,step_key,path),
	FOREIGN KEY(operation_id,step_key) REFERENCES operation_steps(operation_id,step_key)
);
CREATE TABLE deletion_intents (
	deleted_at TEXT,
	path TEXT NOT NULL,
	operation_id TEXT NOT NULL,
	step_key TEXT NOT NULL,
	receipt_hash TEXT NOT NULL,
	FOREIGN KEY(operation_id,step_key,path) REFERENCES deletion_history(operation_id,step_key,path)
);
CREATE UNIQUE INDEX current_deletion_paths ON deletion_intents(path) WHERE deleted_at IS NULL;
CREATE TABLE deletion_nodes (
	operation_id TEXT NOT NULL,
	step_key TEXT NOT NULL,
	receipt_path TEXT NOT NULL,
	relative_path TEXT NOT NULL,
	device INTEGER NOT NULL,
	inode INTEGER NOT NULL,
	mode INTEGER NOT NULL CHECK(mode BETWEEN 0 AND 4294967295),
	PRIMARY KEY(operation_id,step_key,receipt_path,relative_path),
	FOREIGN KEY(operation_id,step_key,receipt_path) REFERENCES deletion_history(operation_id,step_key,path)
);

CREATE TABLE journal (
	seq INTEGER PRIMARY KEY AUTOINCREMENT,
	signed_payload BLOB NOT NULL,
	hash TEXT NOT NULL
);
