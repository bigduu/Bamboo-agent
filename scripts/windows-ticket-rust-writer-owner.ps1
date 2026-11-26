$TicketRustWriterTest = 'store::windows_writer::tests::native_standard_user_scope_and_writer'

function Get-TicketRustWriterBinary {
    $workspace = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
    Push-Location -LiteralPath $workspace
    try {
        $compiler = @(& rustc -vV)
        if ($LASTEXITCODE -ne 0 -or $compiler -cnotcontains 'host: x86_64-pc-windows-msvc') {
            throw 'The Rust writer probe requires the native Windows x64 compiler.'
        }
        $compiler | ForEach-Object { Write-Host $_ }
        $lines = @(& cargo test --locked -p bamboo-tickets --lib --no-run --message-format=json-render-diagnostics 2>&1)
        if ($LASTEXITCODE -ne 0) {
            $lines | Select-Object -Last 24 | ForEach-Object { Write-Host $_ }
            throw 'Could not build the exact Bamboo Ticket library test executable.'
        }
        $units = @()
        $completed = $false
        foreach ($line in $lines) {
            $text = $line.ToString().Trim()
            if (-not $text.StartsWith('{')) { continue }
            $message = $text | ConvertFrom-Json -AsHashtable -ErrorAction Stop
            if ($message.reason -eq 'build-finished' -and $message.success -eq $true) { $completed = $true }
            if ($message.reason -eq 'compiler-artifact' -and
                $message.target.name -ceq 'bamboo_tickets' -and $message.target.kind -contains 'lib' -and
                $message.profile.test -eq $true -and $message.executable) {
                $units += $message
            }
        }
        if (-not $completed -or $units.Count -ne 1) {
            throw 'Cargo did not report one completed Bamboo Ticket library test executable.'
        }
        $unit = $units[0]
        $manifest = Join-Path $workspace 'crates/infra/bamboo-tickets/Cargo.toml'
        if (-not [string]::Equals([IO.Path]::GetFullPath($unit.manifest_path), $manifest,
            [StringComparison]::OrdinalIgnoreCase)) {
            throw 'The reported library test belongs to an unexpected manifest.'
        }
        $target = if ($env:CARGO_TARGET_DIR) { $env:CARGO_TARGET_DIR } else { Join-Path $workspace 'target' }
        $targetPrefix = [IO.Path]::TrimEndingDirectorySeparator([IO.Path]::GetFullPath($target)) +
            [IO.Path]::DirectorySeparatorChar
        $binary = Get-Item -LiteralPath $unit.executable
        if ($binary.PSIsContainer -or ($binary.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or
            $binary.Extension -cne '.exe' -or
            -not $binary.FullName.StartsWith($targetPrefix, [StringComparison]::OrdinalIgnoreCase)) {
            throw 'The reported library test is not a regular executable in the build target directory.'
        }
        return $binary.FullName
    } finally {
        Pop-Location
    }
}

function Assert-TicketRustWriterTestOutput {
    param([string]$Output)

    $lines = $Output -split '\r?\n'
    if (@($lines | Where-Object { $_ -ceq 'running 1 test' }).Count -ne 1 -or
        @($lines | Where-Object { $_ -ceq "test $TicketRustWriterTest ... ok" }).Count -ne 1 -or
        @($lines | Where-Object { $_ -cmatch '^test result: ok\. 1 passed; 0 failed; 0 ignored;' }).Count -ne 1) {
        throw 'The staged Rust executable did not run and pass exactly the intended test.'
    }
}

function Test-TicketRustWriterFileIdentity($Observed, $Expected) {
    return $Observed.file_id -cmatch '^[0-9a-f]{32}$' -and
        $Observed.volume_serial -cmatch '^[0-9a-f]{16}$' -and
        $Observed.file_id -ceq $Expected.file_id -and
        $Observed.volume_serial -ceq $Expected.volume_serial
}

function Assert-RustWriterProbe($Receipt, [int]$OwnerProcessId) {
    if ($Receipt.probe -cne 'rust-writer' -or $Receipt.case -cne 'native_standard_user_scope_and_writer' -or
        $Receipt.owner_pid -ne $OwnerProcessId -or $Receipt.verdict -cne 'available') {
        throw 'The Rust receipt did not identify the exact standard-user writer test.'
    }
    $proof = $Receipt.writer
    $positive = $proof.positive
    if ($positive.pid -ne $OwnerProcessId -or $positive.win32_error -ne 0 -or
        $positive.file.volume_serial -cne $Receipt.volume.volume_serial -or
        $positive.scope.volume_serial -cne $positive.file.volume_serial -or
        $proof.held.win32_error -ne 33 -or $proof.released.win32_error -ne 0 -or
        $proof.held.pid -eq $OwnerProcessId -or $proof.held.pid -ne $proof.released.pid -or
        -not (Test-TicketRustWriterFileIdentity $proof.final_file $positive.file)) {
        throw 'The Rust writer result did not attest independent same-lock contention and reacquisition.'
    }
    foreach ($observation in @($proof.held, $proof.released)) {
        if (-not (Test-TicketRustWriterFileIdentity $observation.scope $positive.scope) -or
            -not (Test-TicketRustWriterFileIdentity $observation.file $positive.file)) {
            throw 'The Rust writer child observed a different scope or lock identity.'
        }
    }
    $token = $proof.contender_token
    if ($token.sid -cne $Receipt.token.sid -or $token.elevated -ne $false -or
        $token.integrity_sid -cne 'S-1-16-8192' -or $token.administrators_group_present -ne $false -or
        $token.local_account_group_present -ne $true -or @($token.excess_privileges_present).Count -ne 0) {
        throw 'The Rust writer contender did not attest the same standard-user token.'
    }
    $links = $proof.hard_link
    if ($links.create_hard_link_win32_error -ne 0 -or $links.raw_alias_lock_contended_win32_error -ne 33 -or
        $links.single_link.link_count -ne 1 -or $links.single_link.win32_error -ne 0 -or
        $links.rejection.stage -cne 'single-link-validation' -or $links.rejection.kind -cne 'InvalidData' -or
        $null -ne $links.rejection.raw_os_error -or $links.rejection.guard_returned -ne $false -or
        $links.rejection.lock_attempted -ne $false) {
        throw 'The Rust writer did not attest the native link and guarded refusal controls.'
    }
    $witness = $links.before.original.bytes_hex
    foreach ($entry in @($links.before.original, $links.before.alias, $links.after.original, $links.after.alias)) {
        if ($entry.link_count -ne 2 -or $entry.bytes_hex -cne $witness -or
            $entry.bytes_hex -cnotmatch '^(?:[0-9a-f]{2})+$' -or
            -not (Test-TicketRustWriterFileIdentity $entry.file $links.single_link.file)) {
            throw 'The Rust hard-link control changed the physical file, link count, or witness bytes.'
        }
    }
    $gate = $Receipt.file_store_gate
    if ($gate.platform_supported -ne $false -or $gate.missing_scope_remained_absent -ne $true -or
        $gate.fixture.file_id -cne $Receipt.volume.fixture_directory_file_id -or
        $gate.fixture.volume_serial -cne $Receipt.volume.volume_serial -or
        @($gate.entries_before).Count -ne 0 -or @($gate.entries_after).Count -ne 0 -or
        [string]::IsNullOrWhiteSpace($gate.existing_scope_error) -or
        $gate.existing_scope_error -cne $gate.missing_scope_error) {
        throw 'The public FileStore Windows refusal lacks an unchanged scope and absent-path control.'
    }
}
