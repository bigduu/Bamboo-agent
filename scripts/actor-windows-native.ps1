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

function New-ActorNativeContext($Workspace, $Evidence) {
  $workspacePath = (Resolve-Path -LiteralPath $Workspace).Path
  $identity = Get-ActorNativeIdentity $workspacePath
  return @{
    schemaVersion = 1
    workspace = $workspacePath
    targetRoot = Join-Path $workspacePath 'target'
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
