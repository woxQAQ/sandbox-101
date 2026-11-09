#!/usr/bin/env nu
# Required-native OCI/EROFS materialization gate.
#
# This wrapper never modifies source files, host tools, or the caller's
# environment. Every invocation writes its evidence to a fresh, per-run
# subdirectory of build/native-gate/ (gitignored); earlier runs are never
# removed or overwritten.
#
# Modes:
#   default     Positive gate. Runs `make test-native` (pure Go JSON stdout),
#               requires exit 0 plus pass events for the four required-native
#               tests, and rejects any required-native skip except the exact
#               documented root identity-mapping exception.
#   --preflight Root-only environment proof. Records tool versions and their
#               pinned /nix/store sources, proves real new user/mount/PID/
#               network namespaces via pinned bubblewrap, proves the full root
#               identity mapping via pinned unshare, then runs one merged-OCI
#               required test through the production mapped sandbox helper.
#               Preflight never replaces the full gate.
#   --negative  Self-check. Builds a command-local PATH view with only bwrap
#               removed and requires the single required layered test to fail
#               with a nonzero exit and the exact missing-tool diagnostic. The
#               view lives only inside this invocation's own log directory and
#               is removed as soon as the command finishes, so no symlink farm
#               is ever archived as an artifact.

const REQUIRED_TESTS = [
  TestActualLayeredEROFSImportAndExtraction
  TestActualSharedOCIHardlinksAndMetadata
  TestActualMergedOCIHardlinksAndMetadata
  TestGCActualLayerCacheAndCompletedImport
]
const ROOT_SKIP_EXCEPTION_TEST = "TestActualMergedOCIInvalidInputsDoNotPublish/unmapped_owner"
const ROOT_SKIP_EXCEPTION_REASON = "root can identity-map all owners"
const MISSING_BWRAP_DIAGNOSTIC = "required OCI tool missing: bwrap"
const NAMESPACE_PROOFS = [user mnt pid net]

# Strict Go test JSON parsing. Explicit empty-line policy: blank or
# whitespace-only stdout lines are ignored; every other line must be a single
# JSON object (a Go test event). Invalid lines are reported by line number
# only, never by content, and always fail the gate, so a mixed output (for
# example a Make echo next to four pass events) can never be accepted.
def parse-events [text: string] {
  let parsed = ($text | lines | enumerate | each {|row|
    if (($row.item | str trim) == "") {
      {kind: empty}
    } else {
      let value = (try { $row.item | from json --strict } catch { null })
      if ($value != null) and (($value | describe | str starts-with "record")) {
        {kind: event, event: $value}
      } else {
        {kind: invalid, line: ($row.index + 1)}
      }
    }
  })
  {
    events: ($parsed | where kind == event | each {|p| $p.event })
    invalid: ($parsed | where kind == invalid | each {|p| $p.line })
  }
}

def require-pure-json [name: string, parsed: record] {
  if ($parsed.invalid | is-not-empty) {
    fail ($"($name) stdout is not pure Go test JSON: "
      + $"($parsed.invalid | length) unexpected non-empty lines at line "
      + $"numbers ($parsed.invalid | str join ', '); line contents are "
      + "withheld, see the saved stdout log")
  }
}

# A Go skip diagnostic has the shape "    <file>.go:<line>: <reason>". Return
# the whitespace-normalized reason only when that source-location prefix is
# present; any other shape yields null, so a reason that merely contains the
# expected phrase somewhere can never satisfy the exception.
def skip-reason [output: string] {
  let parsed = ($output | str trim | parse --regex '^\S+\.go:\d+:\s*(?P<reason>.*)$')
  if ($parsed | is-empty) { return null }
  ($parsed | get 0 | get reason | split row --regex '\s+' | where {|w| $w != "" } | str join " ")
}

def save-outcome [logdir: string, name: string, outcome: record] {
  $outcome.stdout | save --force ($logdir | path join $"($name).stdout.json")
  $outcome.stderr | save --force ($logdir | path join $"($name).stderr.log")
  $outcome.exit_code | into string | save --force ($logdir | path join $"($name).exit_code")
}

def missing-passes [events: list] {
  let passed = ($events
    | where {|e| ($e | get -o Action) == "pass" and (($e | get -o Test) != null) }
    | each {|e| $e | get Test }
    | uniq)
  $REQUIRED_TESTS | where {|t| $t not-in $passed }
}

def is-native-test [name: string] {
  ($name | str starts-with "TestActual") or ($name | str starts-with "TestGCActual")
}

def fail [message: string] {
  print --stderr $"[native-gate] FAIL: ($message)"
  exit 1
}

def run-positive [logdir: string] {
  print "[native-gate] running: make test-native"
  let outcome = (^make test-native | complete)
  save-outcome $logdir "test-native" $outcome

  let parsed = (parse-events $outcome.stdout)
  require-pure-json "make test-native" $parsed
  let events = $parsed.events
  if ($events | is-empty) {
    fail "test-native produced no parseable Go test JSON; see the saved stdout log"
  }
  let missing = (missing-passes $events)

  let skipped = ($events
    | where {|e| ($e | get -o Action) == "skip" and (($e | get -o Test) != null) }
    | each {|e| $e | get Test }
    | uniq)
  let native_skips = ($skipped | where {|t| is-native-test $t })
  let unexpected_skips = ($native_skips | where {|t| $t != $ROOT_SKIP_EXCEPTION_TEST })
  let exception_seen = ($ROOT_SKIP_EXCEPTION_TEST in $native_skips)
  # The exception applies only to the exact test name, only when its
  # source-located skip diagnostic carries the exact normalized reason, and
  # only in a root context. A reason merely containing the phrase, or the
  # same skip at a nonzero effective UID, fails the gate.
  let exception_reason_exact = ($events
    | where {|e| (((($e | get -o Test) | default "") == $ROOT_SKIP_EXCEPTION_TEST)
      and ((($e | get -o Output) | default "") != "")) }
    | each {|e| skip-reason ($e | get Output) }
    | where {|r| $r != null and $r == $ROOT_SKIP_EXCEPTION_REASON }
    | is-not-empty)
  let effective_root = ((^id -u | str trim) == "0")
  let other_skips = ($skipped | where {|t| not (is-native-test $t) })

  ($other_skips | str join "\n") | save --force ($logdir | path join "skips-other.txt")

  let problems = ([
    (if $outcome.exit_code != 0 { [$"make test-native exited ($outcome.exit_code)"] } else { [] })
    (if ($missing | is-not-empty) { [$"required-native tests without a pass event: ($missing | str join ', ')"] } else { [] })
    (if ($unexpected_skips | is-not-empty) { [$"required-native skips are gate failures: ($unexpected_skips | str join ', ')"] } else { [] })
    (if $exception_seen and not $exception_reason_exact { [$"($ROOT_SKIP_EXCEPTION_TEST) skipped without the exact source-located reason '($ROOT_SKIP_EXCEPTION_REASON)'"] } else { [] })
    (if $exception_seen and $exception_reason_exact and not $effective_root { [$"($ROOT_SKIP_EXCEPTION_TEST) used the root identity-mapping skip while the effective UID is not 0"] } else { [] })
  ] | flatten)

  let report = ([
    $"command: make test-native"
    $"exit_code: ($outcome.exit_code)"
    $"required pass events: (($REQUIRED_TESTS | length) - ($missing | length))/($REQUIRED_TESTS | length)"
    $"root skip exception observed=($exception_seen) exact_reason_verified=($exception_reason_exact) root_context=($effective_root)"
    $"other non-native skips recorded separately: ($other_skips | length)"
    $"logs: ($logdir)"
  ] | str join "\n")
  print $"[native-gate] ($report)"
  $"($report)\n" | save --force ($logdir | path join "gate-report.txt")

  if ($problems | is-not-empty) {
    fail ($problems | str join "; ")
  }
  print "[native-gate] PASS: required native OCI/EROFS scenarios verified"
}

def run-negative [logdir: string] {
  # A PATH view valid only for this invocation: everything the caller had
  # except bwrap. Host tools are never touched. The view lives inside this
  # invocation's own log directory and is removed as soon as the command
  # finishes, so it is never archived as an artifact.
  let view = ($logdir | path join "no-bwrap-bin")
  mkdir $view
  for dir in ($env.PATH | reverse) {
    if (($dir | path type) == "dir") {
      for entry in (ls $dir | where {|e| $e.type != "dir" }) {
        let name = ($entry.name | path basename)
        if $name != "bwrap" {
          ^ln -sfn $entry.name ($view | path join $name)
        }
      }
    }
  }
  if (ls $view | where {|e| ($e.name | path basename) == "bwrap" } | is-not-empty) {
    fail "negative setup error: bwrap still present in the PATH view"
  }
  if not ($view | path join "go" | path exists) {
    fail "negative setup error: go missing from the PATH view"
  }

  print "[native-gate] negative check: required layered test without bwrap must fail"
  let outcome = (with-env {PATH: $view, SANDBOX_REQUIRE_EROFS_TESTS: "1", GOWORK: "off"} {
    ^go -C engine test -json -run '^TestActualLayeredEROFSImportAndExtraction$' -count=1 '-timeout=60s' .
  } | complete)
  # Remove only this invocation's own tool view; no other run is touched.
  rm --recursive --force $view
  if ($view | path exists) {
    fail "negative setup error: could not remove the command-local PATH view"
  }
  save-outcome $logdir "negative" $outcome

  if $outcome.exit_code == 0 {
    fail "negative check invalid: the test passed without bwrap"
  }
  let parsed = (parse-events $outcome.stdout)
  require-pure-json "negative go test" $parsed
  let diag_events = ($parsed.events
    | where {|e| (((($e | get -o Test) | default "") == "TestActualLayeredEROFSImportAndExtraction")
      and (((($e | get -o Output) | default "") | str contains $MISSING_BWRAP_DIAGNOSTIC))) })
  if ($diag_events | is-empty) {
    fail $"negative check invalid: exit ($outcome.exit_code) without the exact '($MISSING_BWRAP_DIAGNOSTIC)' diagnostic; a build failure or another missing tool does not count"
  }
  print $"[native-gate] PASS: missing bwrap fails with exit ($outcome.exit_code) and the exact diagnostic"
}

def pinned-path [tool: string] {
  let found = (which $tool)
  if ($found | is-empty) {
    fail $"preflight: required tool not on PATH: ($tool)"
  }
  (($found | get 0).path)
}

def run-preflight [logdir: string] {
  if ((^id -u | str trim) != "0") {
    fail "preflight requires effective UID 0; run it only inside the disposable CI runner through the sudo wrapper"
  }

  let bwrap_bin = (pinned-path bwrap)
  let unshare_bin = (pinned-path unshare)
  let id_bin = (pinned-path id)
  let readlink_bin = (pinned-path readlink)
  let nu_bin = (pinned-path nu)
  for tool in [bwrap mkfs.erofs fsck.erofs dump.erofs umoci go] {
    pinned-path $tool | ignore
  }
  # unshare/id are asserted by contract; nu executes inside the namespace
  # where only /nix/store is bound, so it must be pinned too.
  for tool_record in [{name: unshare, path: $unshare_bin}, {name: id, path: $id_bin}, {name: nu, path: $nu_bin}] {
    if not ($tool_record.path | str starts-with "/nix/store/") {
      fail $"preflight: ($tool_record.name) must come from the pinned Nix closure, got ($tool_record.path)"
    }
  }

  let tool_report = ([
    $"bwrap: (($bwrap_bin)) (^$bwrap_bin --version | str trim)"
    $"mkfs.erofs: ((pinned-path mkfs.erofs)) (^mkfs.erofs --version | lines | first)"
    $"fsck.erofs: ((pinned-path fsck.erofs)) (^fsck.erofs --version | lines | first)"
    $"dump.erofs: ((pinned-path dump.erofs)) (^dump.erofs --version | lines | first)"
    $"umoci: ((pinned-path umoci)) (^umoci --version | lines | first)"
    $"unshare: ($unshare_bin) (^$unshare_bin --version | str trim)"
    $"id: ($id_bin) (^$id_bin --version | lines | first)"
    $"nu: ($nu_bin) (^$nu_bin --version | str trim)"
    $"go: ((pinned-path go)) (^go version | str trim)"
  ] | str join "\n")
  print $"[native-gate] preflight tools:\n($tool_report)"
  $"($tool_report)\n" | save --force ($logdir | path join "preflight.tools.txt")

  # Shared-namespace proof: brand-new user/mount/PID/network namespaces with
  # /nix/store readonly, private /proc and /dev, and a scratch /tmp. Every
  # one of the four inner namespace IDs must differ from its outer peer.
  let outer_ns = ($NAMESPACE_PROOFS | each {|ns| ^$readlink_bin $"/proc/self/ns/($ns)" | str trim })
  let shared = (with-env {GATE_ID: $id_bin, GATE_READLINK: $readlink_bin} {
    (^$bwrap_bin --unshare-all --uid 0 --gid 0
      --ro-bind "/nix/store" "/nix/store" --proc "/proc" --dev "/dev" --tmpfs "/tmp"
      -- $nu_bin --no-config-file --commands '
        print (^$env.GATE_ID -u | str trim)
        print (^$env.GATE_READLINK /proc/self/ns/user | str trim)
        print (^$env.GATE_READLINK /proc/self/ns/mnt | str trim)
        print (^$env.GATE_READLINK /proc/self/ns/pid | str trim)
        print (^$env.GATE_READLINK /proc/self/ns/net | str trim)')
  } | complete)
  let shared_lines = ($shared.stdout | lines)
  let inner_uid = ($shared_lines | get -o 0 | default "")
  let ns_pairs = ($NAMESPACE_PROOFS | enumerate | each {|row|
    {
      name: $row.item
      outer: ($outer_ns | get $row.index)
      inner: ($shared_lines | get -o ($row.index + 1) | default "")
    }
  })
  let ns_report = ($ns_pairs | each {|p| $"($p.name) namespace: outer=($p.outer) inner=($p.inner)" })
  let ns_ok = (($ns_pairs | length) == ($NAMESPACE_PROOFS | length)) and ($ns_pairs | all {|p|
    ($p.inner != "") and ($p.inner != $p.outer)
  })
  let shared_report = ([
    "bwrap --unshare-all --uid 0 --gid 0 --ro-bind /nix/store /nix/store --proc /proc --dev /dev --tmpfs /tmp"
    $"exit_code: ($shared.exit_code)"
    $"inner uid: ($inner_uid)"
    ...$ns_report
    $shared.stderr
  ] | str join "\n")
  $"($shared_report)\n" | save --force ($logdir | path join "preflight.shared-namespace.log")
  if ($shared.exit_code != 0 or ($inner_uid != "0") or not $ns_ok) {
    fail "preflight: shared-namespace check failed; see preflight.shared-namespace.log"
  }

  # Merged-mapping proof: the exact full root identity mapping the production
  # helper uses under effective UID 0. The printed real/effective IDs are
  # validated together with the uid_map/gid_map contents.
  let mapped = (with-env {GATE_ID: $id_bin} {
    (^$unshare_bin --user '--map-users=0:0:4294967295' '--map-groups=0:0:4294967295' --keep-caps
      $nu_bin --no-config-file --commands '
        print $"ruid=(^$env.GATE_ID -ru | str trim) euid=(^$env.GATE_ID -u | str trim) rgid=(^$env.GATE_ID -rg | str trim) egid=(^$env.GATE_ID -g | str trim)"
        print "uid_map:"
        print (open --raw /proc/self/uid_map | str trim)
        print "gid_map:"
        print (open --raw /proc/self/gid_map | str trim)')
  } | complete)
  let mapping_lines = ($mapped.stdout | lines)
  let ids_line = ($mapping_lines | get -o 0 | default "")
  let ids = ($ids_line | parse --regex '^ruid=(?P<ruid>\d+)\s+euid=(?P<euid>\d+)\s+rgid=(?P<rgid>\d+)\s+egid=(?P<egid>\d+)$')
  let ids_ok = (($ids | is-not-empty) and ($ids | get 0 | values | all {|v| $v == "0" }))
  let uid_map_line = ($mapping_lines | skip until {|l| $l == "uid_map:" } | get -o 1 | default "")
  let gid_map_line = ($mapping_lines | skip until {|l| $l == "gid_map:" } | get -o 1 | default "")
  let mapping_report = ([
    "unshare --user --map-users=0:0:4294967295 --map-groups=0:0:4294967295 --keep-caps"
    $"exit_code: ($mapped.exit_code)"
    $"mapped ids validated: ($ids_ok)"
    $mapped.stdout
    $mapped.stderr
  ] | str join "\n")
  $"($mapping_report)\n" | save --force ($logdir | path join "preflight.merged-mapping.log")
  let mapping_ok = ([$uid_map_line $gid_map_line] | all {|line|
    let fields = ($line | split row --regex '\s+')
    (($fields | get -o 0 | default "") == "0"
      and (($fields | get -o 1 | default "") == "0")
      and (($fields | get -o 2 | default "") == "4294967295"))
  })
  if $mapped.exit_code != 0 or not $ids_ok or not $mapping_ok {
    fail "preflight: root identity mapping check failed; see preflight.merged-mapping.log"
  }

  # One required merged-OCI test through the production mapped sandbox helper.
  print "[native-gate] preflight: running required merged-OCI explicit-selection test"
  let outcome = (with-env {SANDBOX_REQUIRE_EROFS_TESTS: "1", GOWORK: "off"} {
    ^go -C engine test -json -run '^TestActualMergedOCIExplicitSelectionAndVerification$' -count=1 '-timeout=60s' .
  } | complete)
  save-outcome $logdir "preflight-merged-test" $outcome
  let parsed = (parse-events $outcome.stdout)
  require-pure-json "preflight merged test" $parsed
  let events = $parsed.events
  let passed = ($events
    | where {|e| ((($e | get -o Action) == "pass")
      and ((($e | get -o Test) | default "") == "TestActualMergedOCIExplicitSelectionAndVerification")) }
    | is-not-empty)
  if $outcome.exit_code != 0 or not $passed {
    fail "preflight: merged-OCI explicit-selection test did not pass; see preflight-merged-test.*"
  }
  print "[native-gate] PASS: preflight environment and mapping proofs complete"
}

def main [--preflight, --negative] {
  if $preflight and $negative {
    print --stderr "[native-gate] choose only one of --preflight/--negative"
    exit 2
  }
  let root = ($env.CURRENT_FILE | path dirname | path dirname)
  cd $root
  let mode_name = (if $preflight { "preflight" } else if $negative { "negative" } else { "positive" })
  let gate_root = ($root | path join "build" "native-gate")
  mkdir $gate_root
  # Fresh per-invocation log directory: refuse to reuse an existing one
  # instead of removing or overwriting any earlier run's evidence.
  let logdir = ($gate_root | path join $"((date now | format date '%Y%m%dT%H%M%S'))-($mode_name)-(random int 100000..999999)")
  if ($logdir | path exists) {
    fail $"refusing to reuse existing log directory: ($logdir)"
  }
  mkdir $logdir
  print $"[native-gate] logs: ($logdir)"

  if $preflight {
    run-preflight $logdir
    return
  }
  if $negative {
    run-negative $logdir
    return
  }
  run-positive $logdir
}
