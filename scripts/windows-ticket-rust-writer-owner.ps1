function Get-TicketRustProbeTest([string] $ProbeCase) {
    switch ($ProbeCase) {
        'rust-writer' { return 'store::windows_writer::tests::native_standard_user_scope_and_writer' }
        'rust-immutable' { return 'store::windows_writer::tests::immutable::native_standard_user_immutable_publication' }
        default { throw 'The Rust Ticket probe case is outside the owner test selection.' }
    }
}

function Get-TicketRustProbeBinary {
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

function Assert-TicketRustProbeTestOutput {
    param([string]$Output, [string]$ExpectedTest)

    $lines = $Output -split '\r?\n'
    if (@($lines | Where-Object { $_ -ceq 'running 1 test' }).Count -ne 1 -or
        @($lines | Where-Object { $_ -ceq "test $ExpectedTest ... ok" }).Count -ne 1 -or
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
    $nameOpen = $proof.lock_name_delete_open
    if ($nameOpen.held_win32_error -ne 32 -or $nameOpen.released_win32_error -ne 0 -or
        -not (Test-TicketRustWriterFileIdentity $nameOpen.released_file $positive.file)) {
        throw 'The held Rust writer did not deny DELETE access to the existing lock name.'
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
    Assert-TicketRustFileStoreGate $Receipt
}

function Assert-TicketRustFileStoreGate($Receipt) {
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

function Test-TicketRustObjectObservation($Observed, $Expected) {
    return $Observed.link_count -eq 1 -and $Expected.link_count -eq 1 -and
        $Observed.bytes_hex -cmatch '^(?:[0-9a-f]{2})+$' -and $Observed.bytes_hex -ceq $Expected.bytes_hex -and
        (Test-TicketRustWriterFileIdentity $Observed.identity $Expected.identity)
}

function Assert-RustImmutableProbe($Receipt, [int]$OwnerProcessId) {
    if ($Receipt.probe -cne 'rust-immutable' -or $Receipt.case -cne 'native_standard_user_immutable_publication' -or
        $Receipt.owner_pid -ne $OwnerProcessId -or $Receipt.verdict -cne 'available') {
        throw 'The Rust receipt did not identify the exact immutable publication test.'
    }
    $proof = $Receipt.immutable
    $writer = $proof.writer
    if ($writer.win32_error -ne 0 -or $writer.link_count -ne 1 -or
        $writer.file.volume_serial -cne $Receipt.volume.volume_serial -or
        $proof.scope.volume_serial -cne $writer.file.volume_serial -or
        $proof.category.volume_serial -cne $writer.file.volume_serial -or
        $proof.category.file_id -cnotmatch '^[0-9a-f]{32}$' -or $proof.category.file_id -ceq $proof.scope.file_id -or
        -not (Test-TicketRustWriterFileIdentity $proof.writer_final_file $writer.file)) {
        throw 'The immutable receipt did not verify the held writer and same-volume category.'
    }
    $insert = $proof.insert
    if ($insert.rename.win32_error -ne 0 -or $insert.rename.ntstatus -cne '00000000' -or
        $insert.stage_absence_win32_error -ne 2 -or $insert.category_flush_win32_error -ne 0 -or
        $insert.published.identity.volume_serial -cne $proof.category.volume_serial -or
        -not (Test-TicketRustObjectObservation $insert.published $insert.stage_before)) {
        throw 'The Rust publication did not preserve the staged file through its create-only rename and barrier.'
    }
    $published = [Convert]::FromHexString($insert.published.bytes_hex)
    $publishedDigest = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($published)).ToLowerInvariant()
    if ($insert.digest -cne $publishedDigest) { throw 'The named Rust object has a different content address.' }

    $collision = $proof.collision
    if ($collision.digest -cne $insert.digest -or $collision.rename.win32_error -ne 183 -or
        $collision.rename.ntstatus -cne 'c0000035' -or $null -ne $collision.category_flush_win32_error -or
        $collision.original_before.identity.file_id -ceq $collision.staged_before.identity.file_id -or
        $collision.staged_before.identity.volume_serial -cne $proof.category.volume_serial -or
        $collision.original_before.bytes_hex -cne $collision.staged_before.bytes_hex -or
        -not (Test-TicketRustObjectObservation $collision.original_before $insert.published) -or
        -not (Test-TicketRustObjectObservation $collision.original_after $collision.original_before) -or
        -not (Test-TicketRustObjectObservation $collision.staged_after $collision.staged_before)) {
        throw 'The Rust collision control did not retain both distinct original and staged objects.'
    }
    $mismatch = $proof.mismatched_stage
    if ($mismatch.digest -cnotmatch '^[0-9a-f]{64}$' -or $mismatch.digest -ceq $insert.digest -or
        $mismatch.error_kind -cne 'InvalidData' -or $null -ne $mismatch.raw_os_error -or
        $mismatch.canonical_alias_stage_error_kind -cne 'InvalidInput' -or $mismatch.canonical_alias_absence_win32_error -ne 2 -or
        $null -ne $mismatch.category_flush_win32_error -or
        $mismatch.target_absence_before_win32_error -ne 2 -or $mismatch.target_absence_after_win32_error -ne 2 -or
        -not (Test-TicketRustObjectObservation $mismatch.before $collision.staged_before) -or
        -not (Test-TicketRustObjectObservation $mismatch.after $mismatch.before)) {
        throw 'The Rust digest-mismatch control did not preserve the stage and absent target.'
    }
    $reuse = $proof.reuse
    if ($reuse.digest -cne $insert.digest -or $reuse.matched -ne $true -or $reuse.category_flush_win32_error -ne 0 -or
        -not (Test-TicketRustObjectObservation $reuse.observed $insert.published) -or
        -not (Test-TicketRustObjectObservation $reuse.after $reuse.observed)) {
        throw 'The Rust reuse control did not verify the existing content and category barrier.'
    }
    $conflict = $proof.conflict
    $expected = [Convert]::FromHexString($conflict.expected_bytes_hex)
    $expectedDigest = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($expected)).ToLowerInvariant()
    if ($conflict.digest -cne $expectedDigest -or $conflict.matched -ne $false -or
        $null -ne $conflict.category_flush_win32_error -or $conflict.observed.bytes_hex -ceq $conflict.expected_bytes_hex -or
        $conflict.observed.identity.volume_serial -cne $proof.category.volume_serial -or
        -not (Test-TicketRustObjectObservation $conflict.observed $conflict.before) -or
        -not (Test-TicketRustObjectObservation $conflict.after $conflict.observed)) {
        throw 'The Rust conflict control did not preserve the wrong bytes and refuse before the category barrier.'
    }
    Assert-TicketRustFileStoreGate $Receipt
}
