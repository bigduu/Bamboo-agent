$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'actor-windows-native.ps1')

function Expect-ActorHarnessFailure([scriptblock]$Body, $Pattern) {
  $caught = $null
  try { & $Body | Out-Null } catch { $caught = $_ }
  if ($null -eq $caught -or $caught.ToString() -notmatch $Pattern) {
    throw "Harness did not fail with the expected reason: $Pattern; got $caught"
  }
}

# These are controller unit fixtures, not the 41 real native acceptance cases.
$workspacePath = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$fixtureName = 'actor-harness-fixture-' + [Guid]::NewGuid().ToString('N')
$targetFixture = Join-Path $workspacePath "target/$fixtureName"
$evidenceFixture = Join-Path $env:RUNNER_TEMP $fixtureName
New-Item -ItemType Directory -Path $targetFixture, $evidenceFixture | Out-Null
try {
  $context = New-ActorNativeContext $workspacePath $evidenceFixture
  $executable = Join-Path $targetFixture 'fixture.exe'
  [IO.File]::WriteAllBytes($executable, [byte[]](1, 2, 3, 4))
  $manifest = Join-Path $workspacePath 'crates/infra/bamboo-storage/Cargo.toml'
  $record = @{
    package = 'bamboo-storage'; snapshot = $executable
    manifest = $manifest; workingDirectory = Split-Path -Parent $manifest
    sha256 = (Get-FileHash -LiteralPath $executable -Algorithm SHA256).Hash
    profile = @{ test = $true }; target = @{ name = 'bamboo_storage'; kind = @('lib') }
  }
  $context.packages['bamboo-storage'] = $record
  $script:observedInvocations = @()
  $listing = {
    param($NativeExecutable, [string[]]$NativeArguments)
    $script:observedInvocations += @{
      executable = $NativeExecutable; arguments = $NativeArguments
      cwd = (Get-Location).Path; manifestDirectory = $env:CARGO_MANIFEST_DIR
    }
    return @{ ExitCode = 0; Output = @('fixture::one: test', '1 test, 0 benchmarks') }
  }
  Assert-ActorNativeList $context 'bamboo-storage' 'fixture::' @('one') 'list.log' $listing
  if ($context.scopes.Count -ne 1 -or $script:observedInvocations.Count -ne 1 -or
      $script:observedInvocations[0].cwd -cne $record.workingDirectory -or
      $script:observedInvocations[0].manifestDirectory -cne $record.workingDirectory -or
      ($script:observedInvocations[0].arguments -join '|') -cne 'fixture::|--list') {
    throw 'Listing did not use the captured binary and Cargo package context'
  }
  $success = {
    param($NativeExecutable, [string[]]$NativeArguments)
    $script:observedInvocations += @{ executable = $NativeExecutable; arguments = $NativeArguments }
    return @{ ExitCode = 0; Output = @('test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s') }
  }
  Assert-ActorNativeRun $context 'bamboo-storage' 'fixture::' 1 'success.log' $success
  if ($script:observedInvocations[1].executable -cne $executable -or
      ($script:observedInvocations[1].arguments -join '|') -cne 'fixture::|--nocapture|--test-threads=1') {
    throw 'Execution did not use exactly the compiled native binary and original selection'
  }
  $nonzero = { return @{ ExitCode = 23; Output = @('native stdout before exit 23', 'native stderr before exit 23', 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s') } }
  Expect-ActorHarnessFailure { Assert-ActorNativeRun $context 'bamboo-storage' 'fixture::' 1 'nonzero.log' $nonzero } 'nonzero.*23'
  $failureLog = Join-Path $evidenceFixture 'nonzero.log'
  if (-not (Test-Path -LiteralPath $failureLog -PathType Leaf)) {
    throw 'Native failure output must be preserved before a nonzero exit is propagated'
  }
  $failureOutput = Get-Content -LiteralPath $failureLog -Raw
  if (-not $failureOutput.Contains('native stdout before exit 23') -or
      -not $failureOutput.Contains('native stderr before exit 23')) {
    throw 'Native failure output lost stdout or stderr'
  }
  $partialOutput = {
    param($NativeExecutable, [string[]]$NativeArguments, $OutputPath)
    if (-not (Test-Path -LiteralPath $OutputPath -PathType Leaf)) {
      throw 'The output log was not created before process invocation'
    }
    [IO.File]::AppendAllText($OutputPath, 'partial output while still running' + [Environment]::NewLine)
    if (-not (Get-Content -LiteralPath $OutputPath -Raw).Contains('partial output while still running')) {
      throw 'Partial process output was not readable before the exit result'
    }
    return @{ ExitCode = 23; Output = @() }
  }
  Expect-ActorHarnessFailure {
    Assert-ActorNativeRun $context 'bamboo-storage' 'fixture::' 1 'partial.log' $partialOutput
  } 'nonzero.*23'
  if (-not (Get-Content -LiteralPath (Join-Path $evidenceFixture 'partial.log') -Raw).Contains('partial output while still running')) {
    throw 'Partial process output was lost when exit checking failed'
  }
  $ignored = { return @{ ExitCode = 0; Output = @('test result: ok. 1 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.01s') } }
  Expect-ActorHarnessFailure { Assert-ActorNativeRun $context 'bamboo-storage' 'fixture::' 1 'ignored.log' $ignored } 'counts did not prove'
  $missing = { return @{ ExitCode = 0; Output = @() } }
  Expect-ActorHarnessFailure { Assert-ActorNativeRun $context 'bamboo-storage' 'fixture::' 1 'missing.log' $missing } 'counts did not prove'
  $emptyResult = { return $null }
  Expect-ActorHarnessFailure { Assert-ActorNativeRun $context 'bamboo-storage' 'fixture::' 1 'empty.log' $emptyResult } 'no exit-code result'
  Expect-ActorHarnessFailure { Assert-ActorNativeRun $context 'bamboo-storage' 'other::' 1 'wrong-selection.log' $success } 'selection differs'
  $extraNames = { return @{ ExitCode = 0; Output = @('fixture::one: test', 'fixture::two: test', '2 tests, 0 benchmarks') } }
  Expect-ActorHarnessFailure { Assert-ActorNativeList $context 'bamboo-storage' 'fixture::' @('one') 'extra-list.log' $extraNames } 'Selected tests differ'
  $beforeTamper = $script:observedInvocations.Count
  [IO.File]::WriteAllBytes($executable, [byte[]](5, 6, 7, 8))
  Expect-ActorHarnessFailure { Assert-ActorNativeRun $context 'bamboo-storage' 'fixture::' 1 'tamper.log' $success } 'executable is missing, replaced or changed'
  if ($script:observedInvocations.Count -ne $beforeTamper) { throw 'A changed binary was executed' }
  [IO.File]::WriteAllBytes($executable, [byte[]](1, 2, 3, 4))
  $outsideRecord = @{} + $record
  $outsideRecord.snapshot = Join-Path $evidenceFixture 'outside.exe'
  [IO.File]::WriteAllBytes($outsideRecord.snapshot, [byte[]](1, 2, 3, 4))
  Expect-ActorHarnessFailure { Invoke-ActorNativeProcess $context $outsideRecord @('fixture::') $success } 'outside its owned directory'
  $changedSource = @{} + $context
  $changedSource.revision = '0000000000000000000000000000000000000000'
  Expect-ActorHarnessFailure { Invoke-ActorNativeProcess $changedSource $record @('fixture::') $success } 'source changed'
  Expect-ActorHarnessFailure { Save-ActorNativeManifest $context } 'all five scopes and all 41 tests'

  # Exercise the real native invocation and exit-code path as well as mocks.
  $commandRecord = @{} + $record
  $commandRecord.snapshot = Join-Path $targetFixture 'command.exe'
  [IO.File]::Copy($env:ComSpec, $commandRecord.snapshot, $false)
  $commandRecord.sha256 = (Get-FileHash -LiteralPath $commandRecord.snapshot -Algorithm SHA256).Hash
  $nativeOutput = @(Invoke-ActorNativeProcess $context $commandRecord @('/d', '/c', 'echo native-controller-ok') $null 'real-success.log')
  if ($nativeOutput.Count -ne 1 -or $nativeOutput[0] -cne 'native-controller-ok') {
    throw 'Real native process output or successful exit was lost'
  }
  Expect-ActorHarnessFailure {
    Invoke-ActorNativeProcess $context $commandRecord @('/d', '/c', 'echo native stdout before exit 23 & echo native stderr before exit 23 1>&2 & exit 23') $null 'real-nonzero.log'
  } 'nonzero.*23'
  $nativeFailureOutput = Get-Content -LiteralPath (Join-Path $evidenceFixture 'real-nonzero.log') -Raw
  if (-not $nativeFailureOutput.Contains('native stdout before exit 23') -or
      -not $nativeFailureOutput.Contains('native stderr before exit 23')) {
    throw 'Real native failure output lost stdout or stderr'
  }

  # Reproduce a runner-style timeout in an owned child controller. Kill only
  # this fixture's process tree after its bounded deadline, then inspect bytes
  # that had to be persisted while the native command was still running.
  $timeoutPayload = Join-Path $targetFixture 'timeout-context.json'
  @{ Context = $context; Record = $commandRecord } |
    ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $timeoutPayload
  $timeoutScriptPath = Join-Path $targetFixture 'timeout-controller.ps1'
  $timeoutScript = @'
param($HelperPath, $PayloadPath)
$ErrorActionPreference = 'Stop'
. $HelperPath
$payload = Get-Content -LiteralPath $PayloadPath -Raw | ConvertFrom-Json -AsHashtable
Invoke-ActorNativeProcess $payload.Context $payload.Record @('/d', '/c', 'echo native timeout stdout & echo native timeout stderr 1>&2 & ping -n 60 127.0.0.1 >nul') $null 'timeout.log'
throw 'The hanging fixture returned before its timeout deadline'
'@
  $timeoutScript | Set-Content -LiteralPath $timeoutScriptPath
  $startInfo = [Diagnostics.ProcessStartInfo]::new()
  $startInfo.FileName = (Get-Process -Id $PID).Path
  $startInfo.UseShellExecute = $false
  $startInfo.RedirectStandardOutput = $true
  $startInfo.RedirectStandardError = $true
  foreach ($argument in @('-NoProfile', '-NonInteractive', '-File', $timeoutScriptPath,
      (Join-Path $PSScriptRoot 'actor-windows-native.ps1'), $timeoutPayload)) {
    $startInfo.ArgumentList.Add($argument)
  }
  $controller = [Diagnostics.Process]::new()
  $controller.StartInfo = $startInfo
  $started = $false
  $stdout = $null
  $stderr = $null
  try {
    $started = $controller.Start()
    if (-not $started) { throw 'Cannot start the isolated timeout controller' }
    $stdout = $controller.StandardOutput.ReadToEndAsync()
    $stderr = $controller.StandardError.ReadToEndAsync()
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(10)
    $retainedWhileRunning = $false
    $timeoutLog = Join-Path $evidenceFixture 'timeout.log'
    while ([DateTimeOffset]::UtcNow -lt $deadline) {
      if ($controller.HasExited) { throw 'The timeout fixture exited before its deadline' }
      if (Test-Path -LiteralPath $timeoutLog -PathType Leaf) {
        $partial = Get-Content -LiteralPath $timeoutLog -Raw
        if ($null -ne $partial -and $partial.Contains('native timeout stdout') -and $partial.Contains('native timeout stderr')) {
          $retainedWhileRunning = $true
        }
      }
      Start-Sleep -Milliseconds 50
    }
    $controller.Kill($true)
    if (-not $controller.WaitForExit(5000)) { throw 'The owned timeout process tree did not exit' }
    if ($controller.ExitCode -eq 0) { throw 'A timed-out controller was incorrectly reported as successful' }
    if (-not $retainedWhileRunning) { throw 'Timed-out native output must be retained before termination' }
    $retainedAfterTimeout = Get-Content -LiteralPath $timeoutLog -Raw
    if (-not $retainedAfterTimeout.Contains('native timeout stdout') -or
        -not $retainedAfterTimeout.Contains('native timeout stderr')) {
      throw 'Timed-out native stdout or stderr was lost after termination'
    }
    Write-Host "Owned timeout fixture retained both streams before/after termination; exit=$($controller.ExitCode)"
  } finally {
    if ($started -and -not $controller.HasExited) {
      $controller.Kill($true)
      if (-not $controller.WaitForExit(5000)) { throw 'Cannot clean the owned timeout fixture' }
    }
    if ($null -ne $stdout) {
      $captured = $stdout.GetAwaiter().GetResult()
      if ($captured) { Write-Host $captured.TrimEnd() }
    }
    if ($null -ne $stderr) {
      $captured = $stderr.GetAwaiter().GetResult()
      if ($captured) { Write-Host $captured.TrimEnd() }
    }
    $controller.Dispose()
  }

  # A complete synthetic manifest must survive the real JSON handoff. Its
  # fixtures still do not execute or claim any of the 41 native Rust cases.
  $complete = @{} + $context
  $complete.packages = @{}
  foreach ($package in @('bamboo-storage', 'bamboo-server', 'bamboo-tools')) {
    $packageRecord = @{} + $record
    $packageRecord.package = $package
    $packageRecord.target = @{ name = $package.Replace('-', '_'); kind = @('lib') }
    $relativeManifest = switch ($package) {
      'bamboo-storage' { 'crates/infra/bamboo-storage/Cargo.toml' }
      'bamboo-server' { 'crates/app/bamboo-server/Cargo.toml' }
      'bamboo-tools' { 'crates/engine/bamboo-tools/Cargo.toml' }
    }
    $packageRecord.manifest = Join-Path $workspacePath $relativeManifest
    $packageRecord.workingDirectory = Split-Path -Parent $packageRecord.manifest
    $complete.packages[$package] = $packageRecord
  }
  $complete.scopes = @()
  foreach ($scope in @(
    @{ package = 'bamboo-storage'; filter = 'snapshot::'; count = 9 }
    @{ package = 'bamboo-server'; filter = 'gateway::'; count = 3 }
    @{ package = 'bamboo-storage'; filter = 'directory::'; count = 19 }
    @{ package = 'bamboo-tools'; filter = 'scoped::'; count = 5 }
    @{ package = 'bamboo-tools'; filter = 'initial::'; count = 5 }
  )) {
    $complete.scopes += @{
      package = $scope.package; filter = $scope.filter
      names = @(1..$scope.count | ForEach-Object { "$($scope.filter)case$_" })
    }
  }
  Save-ActorNativeManifest $complete
  $roundTrip = Read-ActorNativeManifest $evidenceFixture
  if ($roundTrip.revision -cne $complete.revision -or $roundTrip.tree -cne $complete.tree -or
      $roundTrip.packages['bamboo-storage'].sha256 -cne $record.sha256) {
    throw 'Manifest handoff changed the compiled source or binary identity'
  }
  Expect-ActorHarnessFailure { Save-ActorNativeManifest $complete } 'Refuse to replace'
  Write-Output 'Actor native controller unit fixtures passed; no real native acceptance cases were executed by these fixtures.'
} finally {
  Remove-Item -LiteralPath $targetFixture, $evidenceFixture -Recurse -Force
}
