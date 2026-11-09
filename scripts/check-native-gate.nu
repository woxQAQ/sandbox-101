#!/usr/bin/env nu

# Environment preparation belongs to the caller, not to this gate.
const engine_package = "github.com/woxQAQ/sandbox-engine"
const required_tests = [
    TestActualLayeredEROFSImportAndExtraction
    TestActualSharedOCIHardlinksAndMetadata
    TestActualMergedOCIHardlinksAndMetadata
    TestGCActualLayerCacheAndCompletedImport
]
const root_skip = "TestActualMergedOCIInvalidInputsDoNotPublish/unmapped_owner"
const native_tests = '^Test(Actual|GCActual|LayerValidation|LegacyNativeLayered|SharedCached|MergedOCI)'

def ensure [condition: bool, message: string] {
    if not $condition { error make {msg: $message} }
}

# Save all three complete fields before interpreting output or reporting failure.
def capture [logs: path, name: string, command: closure] {
    let result = (do $command | complete)
    $result.stdout | save --raw ($logs | path join $"($name).stdout")
    $result.stderr | save --raw ($logs | path join $"($name).stderr")
    $result.exit_code | into string | save --raw ($logs | path join $"($name).exit-code")
    $result
}

def events [result: record] {
    $result.stdout | lines | each {|line|
        let event = ($line | from json)
        ensure ($event.Action? in [start run pause cont pass bench fail output skip build-output build-fail]) "invalid Go JSON action"
        ensure (($event.Package? | default '') != '') "Go JSON event has no package"
        $event
    }
}

def passed [stream: list, test: string] {
    $stream | any {|event|
        $event.Package == $engine_package and $event.Action == "pass" and $event.Test? == $test
    }
}

def check-positive [logs: path, result: record, required: list, euid: int] {
    let stream = (events $result)
    let skipped = ($stream | where Action == skip | each {|event|
        let output = ($stream | where {|row|
            $row.Package == $event.Package and $row.Test? == $event.Test? and $row.Action == "output"
        } | each {|row| $row.Output? | default '' } | str join)
        {package: $event.Package, test: ($event.Test? | default ''), output: $output}
    })
    $skipped | to json | save ($logs | path join "skips.json")
    for skip in $skipped {
        if $skip.test =~ $native_tests {
            # Match the actual testing.T source diagnostic, not a substring in an
            # arbitrary message or the final "--- SKIP" status line.
            let reasons = ($skip.output | lines | where {|line|
                $line !~ '^(=== RUN|--- SKIP|[ \t]+--- SKIP)'
            })
            let exception = (
                $euid == 0 and $skip.package == $engine_package and $skip.test == $root_skip and
                ($reasons | length) == 1 and
                ($reasons.0 =~ '^[ \t]+image_compat_test\.go:[0-9]+: root can identity-map all owners$')
            )
            ensure $exception $"required native test skipped: ($skip.test)"
        }
    }
    ensure ($result.exit_code == 0) $"test command failed with exit ($result.exit_code)"
    ensure (not ($stream | any {|event| $event.Action in [fail build-fail] })) "Go JSON contains a failure"
    for test in $required {
        ensure (passed $stream $test) $"missing native pass event: ($test)"
    }
}

def executable [name: string] {
    let found = (which $name | where type == external)
    ensure (not ($found | is-empty)) $"required gate tool missing: ($name)"
    $found.0.path | path expand
}

def tool-versions [logs: path] {
    let tools = [
        [name args];
        [go [version]]
        [make [--version]]
        [nu [--version]]
        [golangci-lint [--version]]
        [unshare [--version]]
        [id [--version]]
        [readlink [--version]]
        [bwrap [--version]]
        [mkfs.erofs [--version]]
        [fsck.erofs [--version]]
        [dump.erofs [--version]]
        [umoci [--version]]
    ]
    let versions = ($tools | each {|tool|
        let path = (executable $tool.name)
        let result = (capture $logs $"version-($tool.name)" { ^$path ...$tool.args })
        ensure ($result.exit_code == 0) $"version check failed: ($tool.name)"
        {name: $tool.name, path: $path, version: ($result.stdout + $result.stderr | str trim)}
    })
    $versions | to json | save ($logs | path join "tools.json")
    let erofs = ($versions | where name in [mkfs.erofs fsck.erofs] | each {|tool|
        let version = ($tool.version | parse --regex '\b(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)\b' | first)
        ensure (
            ($version.major | into int) == 1 and
            (($version.minor | into int) > 9 or
                (($version.minor | into int) == 9 and ($version.patch | into int) >= 4))
        ) "erofs-utils must be >=1.9.4 and <2"
        $version
    })
    ensure ($erofs.0 == $erofs.1) "mkfs.erofs and fsck.erofs must match"
    let umoci = ($versions | where name == umoci | first | get version)
    ensure (($umoci | lines | first) == "umoci version 0.6.0") "umoci must be 0.6.0"
}

def readonly-roots [] {
    let configured = ($env.SANDBOX_IMAGE_TOOL_ROOTS? | default '')
    let extra = (if $configured == '' { [] } else { $configured | split row ':' })
    for root in $extra {
        ensure ($root != "/" and ($root | path expand --no-symlink) == $root) $"invalid tool root: ($root)"
        ensure (($root | path type) in [dir symlink]) $"missing tool root: ($root)"
        for reserved in [/proc /dev /tmp /work /tools /input /merged] {
            ensure (
                $root != $reserved and
                not ($root | str starts-with $"($reserved)/") and
                not ($reserved | str starts-with $"($root)/")
            ) $"tool root overlaps private path: ($root)"
        }
    }
    [/usr /bin /lib /lib64] | where {|dir| $dir | path exists } | append $extra
}

def preflight [logs: path, euid: int] {
    ensure ($euid == 0) "native CI preflight requires effective UID 0 on an isolated runner; no automatic sudo"
    tool-versions $logs
    let nu_binary = (executable nu)
    let id = (executable id)
    let readlink = (executable readlink)
    let probe_path = ([$id $readlink] | path dirname | uniq | str join ':')
    let probe = '{
        uid: (^id -ru | into int), euid: (^id -u | into int),
        gid: (^id -rg | into int), egid: (^id -g | into int),
        namespaces: ([user mnt pid net] | each {|ns| ^readlink $"/proc/self/ns/($ns)" | str trim }),
        uid_map: (open --raw /proc/self/uid_map | str trim),
        gid_map: (open --raw /proc/self/gid_map | str trim)
    } | to json --raw'
    let outer = (capture $logs outer { ^$nu_binary --no-config-file -c $probe })
    ensure ($outer.exit_code == 0) "outer identity probe failed"
    let before = ($outer.stdout | from json)
    mut args = [
        --die-with-parent --new-session --unshare-all --uid 0 --gid 0
        --clearenv --setenv LC_ALL C --setenv PATH $probe_path
        --proc /proc --dev /dev --tmpfs /tmp --chdir /
    ]
    for root in (readonly-roots) {
        $args = ($args | append [--ro-bind $root $root])
    }
    let sandbox_args = $args
    let shared = (capture $logs shared-namespace {
        ^bwrap ...$sandbox_args -- $nu_binary --no-config-file -c $probe
    })
    ensure ($shared.exit_code == 0) "shared namespace probe failed"
    let inside = ($shared.stdout | from json)
    ensure ($inside.uid == 0 and $inside.euid == 0 and $inside.gid == 0 and $inside.egid == 0) "shared namespace identity is not root"
    for index in 0..3 {
        ensure (($before.namespaces | get $index) != ($inside.namespaces | get $index)) $"shared namespace unchanged: ($index)"
    }
    let mapped = (capture $logs merged-mapping {
        ^unshare --user --map-users=0:0:4294967295 --map-groups=0:0:4294967295 --keep-caps $nu_binary --no-config-file -c $probe
    })
    ensure ($mapped.exit_code == 0) "merged identity mapping probe failed"
    let mapping = ($mapped.stdout | from json)
    ensure ($mapping.uid == 0 and $mapping.euid == 0 and $mapping.gid == 0 and $mapping.egid == 0) "mapped identity is not root"
    ensure ($mapping.namespaces.0 != $before.namespaces.0) "merged user namespace unchanged"
    for map in [$mapping.uid_map $mapping.gid_map] {
        ensure ($map =~ '^0\s+0\s+4294967295$') "merged mapping must cover all valid Linux IDs"
    }
    let actual = (capture $logs merged-production {
        with-env {SANDBOX_REQUIRE_EROFS_TESTS: "1", GOWORK: "off"} {
            ^go -C engine test -json -run '^TestActualMergedOCIExplicitSelectionAndVerification$' -count=1 -timeout=60s .
        }
    })
    check-positive $logs $actual [TestActualMergedOCIExplicitSelectionAndVerification] $euid
}

def negative [logs: path] {
    let go = (executable go)
    let view = ($logs | path join "tool-view")
    mkdir $view
    try {
        # A private, non-recursive PATH projection preserves compiler wrappers
        # and base tools. No existing executable or directory is changed.
        let sources = ($env.PATH | each {|dir|
            if ($dir | path exists) {
                glob --no-dir ($dir | path join '*') | each {|path|
                    {path: $path, name: ($path | path basename)}
                }
            } else { [] }
        } | flatten | where name != bwrap | uniq-by name | get path)
        let links = (^ln --symbolic ...$sources $view | complete)
        ensure ($links.exit_code == 0) "cannot create missing-bwrap PATH view"
        let result = (with-env {PATH: [$view], SANDBOX_REQUIRE_EROFS_TESTS: "1", GOWORK: "off"} {
            ensure (which bwrap | is-empty) "negative PATH still contains bwrap"
            capture $logs negative {
                ^$go -C engine test -json -run '^TestActualLayeredEROFSImportAndExtraction$' -count=1 -timeout=60s .
            }
        })
        let stream = (events $result)
        ensure ($result.exit_code != 0) "missing bwrap unexpectedly succeeded"
        ensure ($stream | any {|event|
            (
                $event.Package == $engine_package and
                $event.Test? == "TestActualLayeredEROFSImportAndExtraction" and
                $event.Action == "output" and
                (($event.Output? | default '') =~ '^[ \t]+fsmeta_test\.go:[0-9]+: incompatible: required OCI tool missing: bwrap\n$')
            )
        }) "negative command did not report the exact missing-bwrap test diagnostic"
        ensure ($stream | any {|event|
            $event.Test? == "TestActualLayeredEROFSImportAndExtraction" and $event.Action == "fail"
        }) "negative command did not execute and fail the selected test"
    } catch {|err|
        rm --recursive $view
        error make {msg: $err.msg}
    }
    rm --recursive $view
}

def main [
    --preflight # Root-only namespace/mapping and actual merged production probe
    --negative  # Succeed only when the selected required test fails for missing bwrap
] {
    ensure (not ($preflight and $negative)) "choose only one gate mode"
    cd ($env.FILE_PWD | path dirname)
    for dir in [build build/native-gate] {
        ensure (($dir | path type) != symlink) $"refusing symlink log directory: ($dir)"
        mkdir $dir
    }
    let temporary = (^mktemp --directory build/native-gate/run.XXXXXXXXXX | complete)
    ensure ($temporary.exit_code == 0) "cannot create exclusive native gate log directory"
    let logs = ($temporary.stdout | str trim | path expand)
    print $"native gate logs: ($logs)"
    try {
        let euid = (^id -u | into int)
        {euid: $euid, preflight: $preflight, negative: $negative} | to json | save ($logs | path join "invocation.json")
        let git_args = [-c $"safe.directory=($env.PWD)"]
        let source = (capture $logs source { ^git ...$git_args rev-parse HEAD })
        ensure ($source.exit_code == 0) "cannot record source revision"
        let changes = (capture $logs changed-paths { ^git ...$git_args diff --name-only HEAD })
        ensure ($changes.exit_code == 0) "cannot record changed source paths"
        [Makefile scripts/check-native-gate.nu .github/workflows/check.yml] | each {|file|
            {file: $file, sha256: (open --raw $file | hash sha256)}
        } | to json | save ($logs | path join "entrypoints.json")
        # env -i intentionally forwards no HOME or caches. Create private Go
        # state instead of inheriting runner credentials or disabling caching.
        if ($env.HOME? | default '') == '' {
            $env.HOME = ($logs | path join "runtime-home")
            mkdir $env.HOME
            let permissions = (^chmod 0700 $env.HOME | complete)
            ensure ($permissions.exit_code == 0) "cannot protect private Go runtime HOME"
        }
        if $preflight {
            preflight $logs $euid
        } else if $negative {
            negative $logs
        } else {
            let result = (capture $logs test-native { ^make --no-print-directory test-native })
            check-positive $logs $result $required_tests $euid
        }
        "PASS" | save ($logs | path join "result.txt")
        print "native gate PASS"
    } catch {|err|
        $err.msg | save ($logs | path join "result.txt")
        error make {msg: $"native gate FAIL: ($err.msg); logs: ($logs)"}
    }
}
