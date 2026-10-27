param([switch]$RecordHost, [switch]$RecordToolchain, [switch]$CompileAcceptance, [switch]$ExecuteAcceptance, [string]$Workspace, [string]$Evidence)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Get-ActorNativeIdentity($Workspace) {
  $revision = (& git -C $Workspace rev-parse HEAD).Trim()
  if ($LASTEXITCODE -ne 0) { throw 'Cannot read native validation source' }
  $tree = (& git -C $Workspace rev-parse 'HEAD^{tree}').Trim()
  if ($LASTEXITCODE -ne 0) { throw 'Cannot read native validation tree' }
  if ($revision -notmatch '^[0-9a-f]{40}$' -or $tree -notmatch '^[0-9a-f]{40}$') {
    throw 'Invalid native validation source identity'
  }
  if ($env:GITHUB_SHA -and $revision -cne $env:GITHUB_SHA) {
    throw 'Native validation checkout differs from the event source'
  }
  $status = @(& git -C $Workspace status --porcelain --untracked-files=all)
  if ($LASTEXITCODE -ne 0 -or $status.Count -ne 0) { throw 'Native validation source is not clean' }
  return @{ revision = $revision; tree = $tree }
}

function Get-ActorNativePath($Path, $Root) {
  $full = [IO.Path]::GetFullPath($Path)
  $prefix = [IO.Path]::GetFullPath($Root).TrimEnd([IO.Path]::DirectorySeparatorChar) + [IO.Path]::DirectorySeparatorChar
  if (-not $full.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Native artifact path is outside its owned directory: $full"
  }
  return $full
}

function New-ActorNativeContext($Workspace, $Evidence, $TargetRoot = $null) {
  $workspacePath = (Resolve-Path -LiteralPath $Workspace).Path
  $identity = Get-ActorNativeIdentity $workspacePath
  $ownedTarget = if ($TargetRoot) { Get-ActorNativePath $TargetRoot $workspacePath } else { Join-Path $workspacePath 'target' }
  return @{
    schemaVersion = 1
    workspace = $workspacePath
    targetRoot = $ownedTarget
    evidence = (Resolve-Path -LiteralPath $Evidence).Path
    revision = $identity.revision
    tree = $identity.tree
    packages = @{}
    scopes = @()
  }
}

function Assert-ActorNativeArtifact($Context, $Record) {
  $identity = Get-ActorNativeIdentity $Context.workspace
  if ($identity.revision -cne $Context.revision -or $identity.tree -cne $Context.tree) {
    throw 'Native validation source changed after compilation'
  }
  $snapshot = Get-ActorNativePath $Record.snapshot $Context.targetRoot
  $manifest = Get-ActorNativePath $Record.manifest $Context.workspace
  $workingDirectory = Get-ActorNativePath $Record.workingDirectory $Context.workspace
  if ((Split-Path -Parent $manifest) -cne $workingDirectory -or
      -not (Test-Path -LiteralPath $manifest -PathType Leaf)) {
    throw 'Native binary package working directory does not match its manifest'
  }
  $file = Get-Item -LiteralPath $snapshot -ErrorAction Stop
  if ($file.PSIsContainer -or ($file.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or
      $file.Extension -cne '.exe' -or
      (Get-FileHash -LiteralPath $snapshot -Algorithm SHA256).Hash -cne $Record.sha256) {
    throw 'Compiled native executable is missing, replaced or changed'
  }
}

function Invoke-ActorNativeProcess($Context, $Record, [string[]]$Arguments, [scriptblock]$Invoker = $null, [string]$LogName = 'native-process.log') {
  Assert-ActorNativeArtifact $Context $Record
  $logPath = Get-ActorNativePath (Join-Path $Context.evidence $LogName) $Context.evidence
  # Create the log before launching, and never replace earlier evidence.
  [IO.File]::Open($logPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::Read).Dispose()
  $nativeInvoker = $null -eq $Invoker
  if ($nativeInvoker) {
    $Invoker = {
      param($Executable, [string[]]$NativeArguments, $OutputPath)
      $PSNativeCommandUseErrorActionPreference = $false
      & $Executable @NativeArguments 2>&1 | ForEach-Object {
        $line = $_.ToString()
        # Each append opens and closes its writer immediately. A timeout or
        # nonzero exit must retain all output already received from the process.
        [IO.File]::AppendAllText($OutputPath, $line + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
        Write-Host $line
      }
      $code = $LASTEXITCODE
      return @{ ExitCode = $code }
    }
  }
  $previousPath = $env:PATH
  $previousManifest = $env:CARGO_MANIFEST_DIR
  Push-Location -LiteralPath $Record.workingDirectory
  try {
    # Keep the executable beside its original dependency DLLs, and preserve
    # Cargo's package working directory and manifest-directory environment.
    $env:PATH = (Split-Path -Parent $Record.snapshot) + [IO.Path]::PathSeparator + $previousPath
    $env:CARGO_MANIFEST_DIR = $Record.workingDirectory
    $result = & $Invoker $Record.snapshot $Arguments $logPath
    if ($null -eq $result) { throw 'Native process has no exit-code result' }
    if (-not $nativeInvoker) {
      # Injected controller fixtures keep their structured result contract;
      # record that output before checking the simulated process exit too.
      foreach ($item in @($result.Output)) {
        $line = $item.ToString()
        [IO.File]::AppendAllText($logPath, $line + [Environment]::NewLine, [Text.UTF8Encoding]::new($false))
        Write-Host $line
      }
    }
    if ($result.ExitCode -isnot [int] -or $result.ExitCode -ne 0) {
      throw "Native process returned a nonzero or missing exit code: $($result.ExitCode)"
    }
    return @(Get-Content -LiteralPath $logPath)
  } finally {
    Pop-Location
    $env:PATH = $previousPath
    $env:CARGO_MANIFEST_DIR = $previousManifest
  }
}

function Get-ActorNativeBinary($Context, $Package) {
  if ($Context.packages.ContainsKey($Package)) { return $Context.packages[$Package] }
  $log = Join-Path $Context.evidence "compile-$Package.jsonl"
  # Compile each package independently. In particular, do not unify the
  # standalone storage tests with the server's storage/test-utils features.
  $lines = @(& cargo test --locked -p $Package --lib --no-run --message-format=json-render-diagnostics 2>&1 |
    Tee-Object -FilePath $log)
  if ($LASTEXITCODE -ne 0) { throw "Native compilation failed: $Package" }
  $units = @()
  $finished = $false
  foreach ($line in $lines) {
    $text = $line.ToString().Trim()
    if (-not $text.StartsWith('{')) { continue }
    try { $unit = $text | ConvertFrom-Json -AsHashtable -ErrorAction Stop } catch { continue }
    if ($unit.reason -eq 'build-finished' -and $unit.success -eq $true) { $finished = $true }
    if ($unit.reason -eq 'compiler-artifact' -and $unit.target.name -ceq $Package.Replace('-', '_') -and
        $unit.target.kind -contains 'lib' -and $unit.profile.test -eq $true -and $unit.executable) {
      $units += $unit
    }
  }
  if (-not $finished -or $units.Count -ne 1) { throw "No unique completed native lib-test artifact: $Package" }
  $unit = $units[0]
  $executable = Get-ActorNativePath $unit.executable $Context.targetRoot
  $manifest = Get-ActorNativePath $unit.manifest_path $Context.workspace
  $file = Get-Item -LiteralPath $executable -ErrorAction Stop
  if ($file.PSIsContainer -or ($file.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or
      $file.Extension -cne '.exe') { throw 'Cargo did not produce a regular native executable' }
  $hash = (Get-FileHash -LiteralPath $executable -Algorithm SHA256).Hash
  $snapshot = Join-Path $file.DirectoryName "actor-native-$Package-$hash.exe"
  if (Test-Path -LiteralPath $snapshot) {
    if ((Get-FileHash -LiteralPath $snapshot -Algorithm SHA256).Hash -cne $hash) {
      throw 'An existing immutable native executable has different bytes'
    }
  } else { [IO.File]::Copy($executable, $snapshot, $false) }
  $record = @{
    package = $Package; packageId = $unit.package_id; features = @($unit.features)
    originalExecutable = $executable; snapshot = $snapshot; sha256 = $hash
    manifest = $manifest; workingDirectory = Split-Path -Parent $manifest
    profile = $unit.profile; target = $unit.target
  }
  Assert-ActorNativeArtifact $Context $record
  $Context.packages[$Package] = $record
  return $record
}

function Assert-ActorNativeList($Context, $Package, $Filter, $ExpectedNames, $LogName, [scriptblock]$Invoker = $null) {
  $record = Get-ActorNativeBinary $Context $Package
  $output = @(Invoke-ActorNativeProcess $Context $record @($Filter, '--list') $Invoker $LogName)
  $names = @($output | ForEach-Object { if ($_ -match '^(.+): test$') { $Matches[1] } })
  $expected = @($ExpectedNames | ForEach-Object { "$Filter$_" })
  $noun = if ($expected.Count -eq 1) { 'test' } else { 'tests' }
  if ($names.Count -ne $expected.Count -or
      @(Compare-Object ($expected | Sort-Object) ($names | Sort-Object) -CaseSensitive).Count -ne 0 -or
      @($output | Where-Object { $_ -eq "$($expected.Count) $noun, 0 benchmarks" }).Count -ne 1) {
    throw "Selected tests differ from the required native acceptance set: $Filter"
  }
  $Context.scopes += @{ package = $Package; filter = $Filter; names = $expected }
}

function Assert-ActorNativeManifest($Context) {
  if ($Context.schemaVersion -ne 1 -or $Context.packages.Count -ne 3 -or $Context.scopes.Count -ne 5 -or
      @($Context.scopes | ForEach-Object { $_.filter } | Sort-Object -Unique).Count -ne 5 -or
      ($Context.scopes | ForEach-Object { $_.names.Count } | Measure-Object -Sum).Sum -ne 41) {
    throw 'Native acceptance manifest must retain all five scopes and all 41 tests'
  }
  foreach ($package in @('bamboo-storage', 'bamboo-server', 'bamboo-tools')) {
    if (-not $Context.packages.ContainsKey($package)) { throw "Missing native package: $package" }
    $record = $Context.packages[$package]
    if ($record.package -cne $package -or $record.target.name -cne $package.Replace('-', '_') -or
        $record.target.kind -notcontains 'lib' -or $record.profile.test -ne $true) {
      throw "Native package is not its compiled lib-test target: $package"
    }
    Assert-ActorNativeArtifact $Context $record
  }
}

function Save-ActorNativeManifest($Context) {
  Assert-ActorNativeManifest $Context
  $path = Join-Path $Context.evidence 'verified-binaries.json'
  if (Test-Path -LiteralPath $path) { throw 'Refuse to replace an earlier native acceptance manifest' }
  $Context | ConvertTo-Json -Depth 12 | Set-Content $path
}

function Read-ActorNativeManifest($Evidence) {
  $context = Get-Content -LiteralPath (Join-Path $Evidence 'verified-binaries.json') -Raw |
    ConvertFrom-Json -AsHashtable
  if ($context.evidence -cne (Resolve-Path -LiteralPath $Evidence).Path) { throw 'Native evidence directory changed' }
  Assert-ActorNativeManifest $context
  return $context
}

function Assert-ActorNativeRun($Context, $Package, $Filter, $ExpectedCount, $LogName, [scriptblock]$Invoker = $null) {
  $scopes = @($Context.scopes | Where-Object { $_.package -ceq $Package -and $_.filter -ceq $Filter })
  if ($scopes.Count -ne 1 -or $scopes[0].names.Count -ne $ExpectedCount) {
    throw 'Runtime selection differs from the compiled and enumerated native test set'
  }
  # Runtime must never call Cargo: use exactly the frozen, enumerated bytes.
  $output = @(Invoke-ActorNativeProcess $Context $Context.packages[$Package] @($Filter, '--nocapture', '--test-threads=1') $Invoker $LogName)
  $summaries = @($output | Where-Object { $_ -match '^test result:' })
  $passed = "^test result: ok\. $ExpectedCount passed; 0 failed; 0 ignored; 0 measured; [0-9]+ filtered out; finished in .+$"
  if ($summaries.Count -ne 1 -or $summaries[0] -notmatch $passed) {
    throw "Native test counts did not prove $ExpectedCount passed, zero failed/ignored: $Filter"
  }
  "$Filter : $ExpectedCount passed; zero failed/ignored" |
    Tee-Object -FilePath (Join-Path $Context.evidence 'verified-counts.txt') -Append
}

function Write-ActorNativeHostEvidence($Workspace, $Evidence) {
  $evidence = $Evidence
  New-Item -ItemType Directory -Force $evidence | Out-Null
  @(
    "checkout=$(git rev-parse HEAD)"
    "github_sha=$env:GITHUB_SHA"
    "pr_head=$env:PR_HEAD_SHA"
    "runner_os=$env:RUNNER_OS"
    "runner_arch=$env:RUNNER_ARCH"
    "image_os=$env:ImageOS"
    "image_version=$env:ImageVersion"
    "stack=$env:RUST_MIN_STACK"
    "frontend=$env:BAMBOO_FRONTEND_BUILD_MODE"
    "lock_sha256=$((Get-FileHash Cargo.lock -Algorithm SHA256).Hash)"
  ) | Tee-Object -FilePath "$evidence/host.txt"
  Get-CimInstance Win32_OperatingSystem |
    Select-Object Caption, Version, BuildNumber, OSArchitecture |
    Format-List | Out-String | Tee-Object -FilePath "$evidence/host.txt" -Append
  if ($env:RUNNER_OS -ne 'Windows' -or $env:RUNNER_ARCH -ne 'X64') {
    throw 'Acceptance requires native Windows x64'
  }

  # Do not infer junction support from cross-compilation or skipped tests.
  $probe = Join-Path $env:RUNNER_TEMP 'actor-snapshot-junction-probe'
  $target = Join-Path $probe 'target'
  $link = Join-Path $probe 'link'
  New-Item -ItemType Directory -Force $target | Out-Null
  & cmd /C mklink /J $link $target 2>&1 |
    Tee-Object -FilePath "$evidence/junction.txt"
  if ($LASTEXITCODE -ne 0) { throw 'Native junction creation failed' }
  $junction = Get-Item -LiteralPath $link -Force
  $junction | Format-List FullName, LinkType, Target, Attributes |
    Out-String | Tee-Object -FilePath "$evidence/junction.txt" -Append
  if ($junction.LinkType -ne 'Junction') { throw 'Expected a real junction' }
  & fsutil reparsepoint query $link 2>&1 |
    Tee-Object -FilePath "$evidence/junction.txt" -Append
  if ($LASTEXITCODE -ne 0) { throw 'Native junction query failed' }
}

function Write-ActorNativeToolchainEvidence($Workspace, $Evidence) {
  $evidence = $Evidence
  & rustc -vV 2>&1 | Tee-Object -FilePath "$evidence/toolchain.txt"
  if ($LASTEXITCODE -ne 0) { throw 'rustc version failed' }
  $version = Get-Content "$evidence/toolchain.txt"
  if ($version -notcontains 'host: x86_64-pc-windows-msvc' -or
      $version -notcontains 'release: 1.99.0') {
    throw 'Acceptance requires Rust 1.99.0 on native Windows/MSVC'
  }
  & cargo -V 2>&1 | Tee-Object -FilePath "$evidence/toolchain.txt" -Append
  if ($LASTEXITCODE -ne 0) { throw 'cargo version failed' }
}

function Compile-ActorNativeAcceptance($Workspace, $Evidence) {
  $evidence = $Evidence
  $nativeContext = New-ActorNativeContext $Workspace $evidence $env:CARGO_TARGET_DIR
  function Assert-TestList($package, $filter, $expectedNames, $logName) {
    Assert-ActorNativeList $nativeContext $package $filter $expectedNames $logName
  }
  # Keep independent package feature selections, then freeze and list
  # their exact executables. Runtime never asks Cargo to compile again.
  Assert-TestList 'bamboo-storage' 'v2::actor_snapshot_windows_tests::' @(
    'changed_birth_stale_actor_and_root_authority_fail_without_repair'
    'grow_after_open_is_capped_and_actual_bytes_debit_the_aggregate_budget'
    'live_actor_fence_authorizes_self_and_descendants_but_not_siblings_or_foreign_roots'
    'regular_file_symlink_replacement_is_rejected_and_open_handle_retains_original'
    'nested_snapshot_is_private_read_only_and_stable_after_restart'
    'pending_task_and_copy_journals_leave_owned_home_unchanged'
    'retained_directory_and_file_handles_reject_reparse_replacement'
    'stale_identity_and_tight_budgets_fail_without_partial_tree'
    'unsupported_namespaces_selectors_and_object_kinds_fail_without_writes'
  ) 'snapshot-list.log'
  Assert-TestList 'bamboo-server' 'handlers::agent::actor_snapshot::tests::' @(
    'canonical_gateway_requires_trusted_host_owner_and_keeps_codex_response_scope'
    'corrupt_and_unsupported_authority_return_private_typed_errors_without_writes'
    'snapshot_v2_negotiation_is_opt_in_with_isolated_etags_and_native_none_placement'
  ) 'gateway-list.log'
  Assert-TestList 'bamboo-storage' 'v2::actor_directory::tests::' @(
    'active_child_cannot_rebind_its_own_project'
    'active_grandchild_cannot_observe_middle_parent_project_rebind'
    'active_root_allows_one_ui_revision_but_blocks_ambiguous_ui_gap'
    'child_project_must_match_its_current_parent'
    'corrupt_or_mismatched_authority_fails_closed_without_repair'
    'delayed_resident_cleanup_cannot_retire_replacement_activation'
    'duplicate_physical_actor_id_fails_closed_across_stale_stores'
    'expired_owner_cannot_checkpoint_or_finish_new_attempt'
    'host_policy_and_intent_survive_reassignment_and_physical_replacement'
    'independent_stores_serialize_competing_owners'
    'initialized_marker_prevents_lost_sidecar_from_resetting_attempt'
    'only_host_binding_sets_policy_and_live_or_regressive_changes_are_rejected'
    'orphaned_nested_child_cannot_publish_activation_authority'
    'persisted_session_precedes_claim_and_attempt_survives_restart'
    'recreated_middle_parent_cannot_revive_grandchild_fence'
    'retirement_fences_owner_and_preserves_identity'
    'root_project_aba_cannot_revive_an_old_active_fence'
    'root_project_rebinds_only_when_no_activation_is_live'
    'store_opened_before_creation_discovers_root_and_child_from_durable_files'
  ) 'directory-list.log'
  Assert-TestList 'bamboo-tools' 'local_task_resources::windows::tests::local_task_resources_windows_' @(
    'foreground_cancel_reaps_descendants'
    'background_scope_drain'
    'fast_leader_descendants'
    'execution_isolation'
    'assignment_failure_reaps_suspended_child'
  ) 'local-task-process-list.log'
  Assert-TestList 'bamboo-tools' 'local_task_resources::windows::tests::initial_exec_windows_' @(
    'live_cancel_drains_retained_job'
    'cancel_after_leader_reap_drains_retained_job'
    'delivery_closes_last_job_handle_without_killing_descendant'
    'disarm_failure_retains_job_until_cleanup'
    'setup_failure_retains_suspended_child_for_cleanup'
  ) 'initial-exec-process-list.log'
  Save-ActorNativeManifest $nativeContext
}

function Run-ActorNativeAcceptance($Workspace, $Evidence) {
  $evidence = $Evidence
  $nativeContext = Read-ActorNativeManifest $evidence
  function Assert-TestRun($package, $filter, $expectedCount, $logName) {
    Assert-ActorNativeRun $nativeContext $package $filter $expectedCount $logName
  }
  Assert-TestRun 'bamboo-storage' 'v2::actor_snapshot_windows_tests::' 9 'snapshot-run.log'
  Assert-TestRun 'bamboo-server' 'handlers::agent::actor_snapshot::tests::' 3 'gateway-run.log'
  Assert-TestRun 'bamboo-storage' 'v2::actor_directory::tests::' 19 'directory-run.log'
  Assert-TestRun 'bamboo-tools' 'local_task_resources::windows::tests::local_task_resources_windows_' 5 'local-task-process-run.log'
  Assert-TestRun 'bamboo-tools' 'local_task_resources::windows::tests::initial_exec_windows_' 5 'initial-exec-process-run.log'
  # This receipts the selected native reader/process tests, not all of #1338 or #791.
  if ($env:GITHUB_STEP_SUMMARY) { Get-Content "$evidence/verified-counts.txt" | Add-Content $env:GITHUB_STEP_SUMMARY }
}

foreach ($action in @('RecordHost', 'RecordToolchain', 'CompileAcceptance', 'ExecuteAcceptance')) {
  if (Get-Variable -Name $action -ValueOnly) {
    if (-not $Workspace -or -not $Evidence) { throw 'Native acceptance requires explicit workspace and evidence paths' }
    switch ($action) {
      RecordHost { Write-ActorNativeHostEvidence $Workspace $Evidence }
      RecordToolchain { Write-ActorNativeToolchainEvidence $Workspace $Evidence }
      CompileAcceptance { Compile-ActorNativeAcceptance $Workspace $Evidence }
      ExecuteAcceptance { Run-ActorNativeAcceptance $Workspace $Evidence }
    }
  }
}
