function Get-TicketRustProbeTest([string] $ProbeCase) {
    switch ($ProbeCase) {
        'rust-writer' { return 'store::windows_writer::tests::native_standard_user_scope_and_writer' }
        'rust-immutable' { return 'store::windows_writer::tests::immutable::native_standard_user_immutable_publication' }
        'rust-head' { return 'store::windows_writer::tests::head::native_standard_user_head_replacement' }
        'rust-graph' { return 'store::windows_writer::tests::graph::native_standard_user_ticket_graph_publication' }
        'rust-graph-process-exit' { return 'store::windows_writer::tests::graph::process_exit::native_standard_user_ticket_graph_process_exit' }
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

function Test-TicketRustHeadObservation($Observed, $Expected) {
    return $Observed.bytes_hex -cmatch '^(?:3[0-9]|6[1-6]){64}$' -and
        $Observed.bytes_hex -ceq $Expected.bytes_hex -and
        (Test-TicketRustWriterFileIdentity $Observed.identity $Expected.identity)
}

function Assert-RustHeadProbe($Receipt, [int]$OwnerProcessId) {
    if ($Receipt.probe -cne 'rust-head' -or $Receipt.case -cne 'native_standard_user_head_replacement' -or
        $Receipt.owner_pid -ne $OwnerProcessId -or $Receipt.verdict -cne 'available') {
        throw 'The Rust receipt did not identify the exact HEAD replacement test.'
    }
    $proof = $Receipt.head
    if ($proof.writer.win32_error -ne 0 -or $proof.writer.link_count -ne 1 -or
        $proof.writer.file.volume_serial -cne $Receipt.volume.volume_serial -or
        $proof.scope.file_id -cne $Receipt.volume.fixture_directory_file_id -or
        $proof.scope.volume_serial -cne $proof.writer.file.volume_serial -or
        -not (Test-TicketRustWriterFileIdentity $proof.writer_final_file $proof.writer.file)) {
        throw 'The Rust HEAD receipt did not retain the original writable scope and writer identity.'
    }
    $initial = $proof.initial.named
    if ($proof.initial.scope_flush_win32_error -ne 0 -or $initial.link_count -ne 1 -or
        $initial.identity.volume_serial -cne $proof.scope.volume_serial -or
        -not (Test-TicketRustHeadObservation $initial $initial)) {
        throw 'The old Rust HEAD was not published with its expected bytes and scope barrier.'
    }

    foreach ($name in @('no_source_delete', 'no_reader_delete_share', 'replace')) {
        $phase = $proof.$name
        if ($phase.source_delete_access -ne ($name -ne 'no_source_delete') -or
            $phase.reader_delete_share -ne ($name -ne 'no_reader_delete_share') -or
            $phase.rename.information_class -ne 65 -or $phase.rename.flags -ne 3 -or
            $phase.named_before.link_count -ne 1 -or $phase.named_after.link_count -ne 1 -or
            $phase.staged_before.link_count -ne 1 -or $phase.staged_before.identity.volume_serial -cne $proof.scope.volume_serial -or
            $phase.staged_before.identity.file_id -ceq $initial.identity.file_id -or
            $phase.staged_before.bytes_hex -ceq $initial.bytes_hex -or
            -not (Test-TicketRustHeadObservation $phase.old_reader_before $initial) -or
            -not (Test-TicketRustHeadObservation $phase.old_reader_after $phase.old_reader_before) -or
            -not (Test-TicketRustHeadObservation $phase.named_before $initial)) {
            throw "The Rust HEAD identities or sharing controls were invalid for '$name'."
        }
        if ($name -eq 'replace') {
            if ($phase.rename.win32_error -ne 0 -or $phase.rename.ntstatus -cne '00000000' -or
                $phase.scope_flush_win32_error -ne 0 -or $phase.stage_absence_win32_error -ne 2 -or
                -not (Test-TicketRustHeadObservation $phase.named_after $phase.staged_before)) {
                throw 'The Rust HEAD replacement did not publish the staged successor and flush its scope.'
            }
        } else {
            $win32 = if ($name -eq 'no_source_delete') { 5 } else { 32 }
            $status = if ($name -eq 'no_source_delete') { 'c0000022' } else { 'c0000043' }
            if ($phase.rename.win32_error -ne $win32 -or $phase.rename.ntstatus -cne $status -or
                $null -ne $phase.scope_flush_win32_error -or $phase.staged_after.link_count -ne 1 -or
                -not (Test-TicketRustHeadObservation $phase.named_after $initial) -or
                -not (Test-TicketRustHeadObservation $phase.staged_after $phase.staged_before)) {
                throw "The actual denied Rust HEAD rename did not preserve both files at '$name'."
            }
        }
    }
    Assert-TicketRustFileStoreGate $Receipt
}

function Get-TicketRustGraphHash($Observed) {
    if ($Observed.bytes_hex -cnotmatch '^(?:[0-9a-f]{2}){1,2048}$') {
        throw 'The Rust Ticket graph evidence lacked bounded full bytes.'
    }
    $bytes = [Convert]::FromHexString($Observed.bytes_hex)
    return [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData($bytes)).ToLowerInvariant()
}

function Assert-TicketRustGraphRevision($Graph, $Scope, [int]$Sequence, $Parent) {
    if ($Graph.schema -ne 1 -or $Graph.sequence -ne $Sequence -or $Graph.parent -cne $Parent -or
        $Graph.commit_hash -cnotmatch '^[0-9a-f]{64}$' -or $Graph.manifest_hash -cnotmatch '^[0-9a-f]{64}$' -or
        $Graph.snapshot_sha256 -cnotmatch '^[0-9a-f]{64}$') {
        throw 'The Rust Ticket graph did not match the expected schema, sequence, or parent commit.'
    }
    $payload = @{}
    foreach ($kind in @('ticket', 'header', 'manifest', 'commit')) {
        $object = $Graph.objects.$kind
        $category = switch ($kind) {
            'manifest' { 'manifests' }
            'commit'   { 'commits' }
            default    { 'objects' }
        }
        if ($object.category -cne $category -or $object.identity.volume_serial -cne $Scope.volume_serial -or
            -not (Test-TicketRustObjectObservation $object $object) -or
            $object.address -cne (Get-TicketRustGraphHash $object)) {
            throw "The Rust Ticket $kind did not attest its named bytes and physical identity."
        }
        $bytes = [Convert]::FromHexString($object.bytes_hex)
        $payload[$kind] = [Text.Encoding]::UTF8.GetString($bytes) | ConvertFrom-Json -AsHashtable
    }
    $title = if ($Sequence -eq 1) { 'First checkpoint' } else { 'Final checkpoint' }
    if ($Graph.commit_hash -cne $Graph.objects.commit.address -or
        $Graph.manifest_hash -cne $Graph.objects.manifest.address -or
        $payload.commit.schema -ne $Graph.schema -or $payload.commit.seq -ne $Graph.sequence -or
        $payload.commit.parent -cne $Graph.parent -or $payload.commit.manifest -cne $Graph.manifest_hash -or
        $payload.manifest.schema -ne $Graph.schema -or $payload.manifest.objects.Count -ne 2 -or
        $payload.manifest.objects.header -cne $Graph.objects.header.address -or
        $payload.manifest.objects['tickets:graph-ticket'] -cne $Graph.objects.ticket.address -or
        $payload.header.schema -ne $Graph.schema -or $payload.header.seq -ne $Graph.sequence -or
        $Graph.binding.scope_id -cne 'graph-scope' -or
        $Graph.binding.supervisor_session_id -cne 'graph-supervisor' -or $Graph.binding.binding_revision -ne 1 -or
        $payload.header.binding.scope_id -cne $Graph.binding.scope_id -or
        $payload.header.binding.supervisor_session_id -cne $Graph.binding.supervisor_session_id -or
        $payload.header.binding.binding_revision -ne $Graph.binding.binding_revision -or
        $payload.ticket.id -cne 'graph-ticket' -or $payload.ticket.scope_id -cne $Graph.binding.scope_id -or
        $payload.ticket.updated_seq -ne $Sequence -or $payload.ticket.contract.title -cne $title) {
        throw 'The actual Rust Ticket commit, manifest and objects were not linked to the reported scope and revision.'
    }
}

function Assert-TicketRustGraphPublication($Revision) {
    $kinds = @('ticket', 'header', 'manifest', 'commit')
    $publications = @($Revision.publications)
    if ($publications.Count -ne $kinds.Count -or $Revision.head_scope_flush_win32_error -ne 0) {
        throw 'The Rust Ticket publication lacked its four ordered immutable names and final scope barrier.'
    }
    for ($index = 0; $index -lt $kinds.Count; $index++) {
        $kind = $kinds[$index]
        $publication = $publications[$index]
        $object = $Revision.graph.objects.$kind
        if ($publication.kind -cne $kind -or $publication.category -cne $object.category -or
            $publication.address -cne $object.address -or
            -not (Test-TicketRustWriterFileIdentity $publication.stage_identity $object.identity) -or
            $publication.file_flush_win32_error -ne 0 -or $publication.rename.ntstatus -cne '00000000' -or
            $publication.rename.win32_error -ne 0 -or $publication.rename.information_class -ne 10 -or
            $publication.rename.flags -ne 0 -or $publication.stage_absence_win32_error -ne 2 -or
            $publication.category_flush_win32_error -ne 0) {
            throw "The Rust Ticket $kind did not attest the staged file, create-only rename and category barrier."
        }
    }
    $headBytes = [Convert]::ToHexString([Text.Encoding]::ASCII.GetBytes($Revision.graph.commit_hash)).ToLowerInvariant()
    if ($Revision.head.link_count -ne 1 -or $Revision.head.bytes_hex -cne $headBytes) {
        throw 'The Rust Ticket publication did not install the actual commit hash as HEAD.'
    }
}

function Assert-TicketRustGraphReopenMatch($Observed, $Published) {
    if ($Observed.commit_hash -cne $Published.commit_hash -or $Observed.manifest_hash -cne $Published.manifest_hash -or
        $Observed.snapshot_sha256 -cne $Published.snapshot_sha256) {
        throw 'The freshly opened Rust Ticket graph did not match the published graph.'
    }
    foreach ($kind in @('ticket', 'header', 'manifest', 'commit')) {
        if (-not (Test-TicketRustObjectObservation $Observed.objects.$kind $Published.objects.$kind)) {
            throw "The freshly opened Rust Ticket $kind did not retain the published identity and bytes."
        }
    }
}

function Assert-RustGraphProbe($Receipt, [int]$OwnerProcessId) {
    if ($Receipt.probe -cne 'rust-graph' -or $Receipt.case -cne 'native_standard_user_ticket_graph_publication' -or
        $Receipt.owner_pid -ne $OwnerProcessId -or $Receipt.verdict -cne 'available') {
        throw 'The Rust receipt did not identify the exact Ticket graph publication test.'
    }
    $proof = $Receipt.graph
    $initial = $proof.initial
    $successor = $proof.successor
    $reopen = $proof.reopen
    if ($proof.scope.file_id -cne $Receipt.volume.fixture_directory_file_id -or
        $proof.scope.volume_serial -cne $Receipt.volume.volume_serial -or
        $proof.writer.file.volume_serial -cne $proof.scope.volume_serial -or
        $proof.writer.win32_error -ne 0 -or $proof.writer.link_count -ne 1 -or
        $reopen.writer.win32_error -ne 0 -or $reopen.writer.link_count -ne 1 -or
        -not (Test-TicketRustWriterFileIdentity $reopen.scope $proof.scope) -or
        -not (Test-TicketRustWriterFileIdentity $reopen.writer.file $proof.writer.file)) {
        throw 'The Rust Ticket graph did not reopen its original physical scope and existing writer lock.'
    }

    Assert-TicketRustGraphRevision $initial.graph $proof.scope 1 $null
    Assert-TicketRustGraphRevision $successor.graph $proof.scope 2 $initial.graph.commit_hash
    Assert-TicketRustGraphPublication $initial
    Assert-TicketRustGraphPublication $successor
    $categoryOrder = 'ticket.category_flush|header.category_flush|manifest.category_flush|commit.category_flush'
    if (($initial.operation_order -join '|') -cne "$categoryOrder|head.seed_scope_flush|head.read_named" -or
        ($successor.operation_order -join '|') -cne "$categoryOrder|head.stage_file_flush|head.replace|head.scope_flush|head.read_named" -or
        $successor.head_rename.ntstatus -cne '00000000' -or $successor.head_rename.win32_error -ne 0 -or
        $successor.head_rename.information_class -ne 65 -or $successor.head_rename.flags -ne 3 -or
        $successor.head_stage_absence_win32_error -ne 2 -or
        $initial.head.identity.volume_serial -cne $proof.scope.volume_serial -or
        $successor.head.identity.volume_serial -cne $proof.scope.volume_serial -or
        $initial.head.identity.file_id -ceq $successor.head.identity.file_id -or
        -not (Test-TicketRustObjectObservation $successor.head $successor.head_stage)) {
        throw 'The Rust Ticket HEAD did not publish the distinct staged successor after the graph barriers.'
    }
    if (($reopen.operation_order -join '|') -cne 'open_scope|acquire_writer|read_head|selected.commit|selected.manifest|selected.header|selected.ticket|previous.commit|previous.manifest|previous.header|previous.ticket' -or
        -not (Test-TicketRustObjectObservation $reopen.head $successor.head)) {
        throw 'The fresh Rust Ticket graph traversal did not start at the published named HEAD.'
    }
    Assert-TicketRustGraphRevision $reopen.selected $reopen.scope 2 $initial.graph.commit_hash
    Assert-TicketRustGraphRevision $reopen.previous $reopen.scope 1 $null
    Assert-TicketRustGraphReopenMatch $reopen.selected $successor.graph
    Assert-TicketRustGraphReopenMatch $reopen.previous $initial.graph

    $corruption = $proof.corruption
    $previousTicket = $reopen.previous.objects.ticket
    $successorTicket = $reopen.selected.objects.ticket
    $stale = $corruption.stale_stage_before
    if ($corruption.kind -cne 'object_hash_mismatch' -or $corruption.address -cne $successorTicket.address -or
        $corruption.observed_sha256 -cne $previousTicket.address -or
        $corruption.observed_sha256 -ceq $corruption.address -or
        (Get-TicketRustGraphHash $corruption.injected) -cne $corruption.observed_sha256 -or
        $corruption.injected.bytes_hex -cne $previousTicket.bytes_hex -or
        -not (Test-TicketRustWriterFileIdentity $corruption.injected.identity $successorTicket.identity) -or
        -not (Test-TicketRustObjectObservation $corruption.object_before $successorTicket) -or
        -not (Test-TicketRustObjectObservation $corruption.object_after $corruption.injected) -or
        -not (Test-TicketRustObjectObservation $corruption.head_before $reopen.head) -or
        -not (Test-TicketRustObjectObservation $corruption.head_after $corruption.head_before) -or
        $corruption.stale_stage_name -cne '.HEAD-graph-stale' -or $stale.identity.volume_serial -cne $proof.scope.volume_serial -or
        $stale.identity.file_id -ceq $reopen.head.identity.file_id -or $stale.bytes_hex -cne $initial.head.bytes_hex -or
        -not (Test-TicketRustObjectObservation $corruption.stale_stage_after $stale) -or
        ($corruption.operation_order -join '|') -cne 'stage_stale_head|read_stale_stage_before|read_head_before|read_writer_before|read_object_before|inject_object_bytes|sync_injected_object|traverse_named_head|reject_object_checksum|read_head_after|read_writer_after|read_object_after|read_stale_stage_after') {
        throw 'The Rust Ticket graph did not refuse the referenced content mismatch and preserve the named files.'
    }
    foreach ($writer in @($corruption.writer_before, $corruption.writer_after)) {
        if ($writer.link_count -ne 1 -or $writer.bytes_hex -cne '' -or
            -not (Test-TicketRustWriterFileIdentity $writer.identity $reopen.writer.file)) {
            throw 'The Rust Ticket corruption control changed the existing writer lock.'
        }
    }
    Assert-TicketRustFileStoreGate $Receipt
}

function Assert-TicketRustGraphProcessRevision($Graph, $Volume, [int]$Sequence, $Parent) {
    if ($Graph.schema -ne 1 -or $Graph.sequence -ne $Sequence -or $Graph.parent -cne $Parent -or
        $Graph.binding.scope_id -cne 'graph-scope' -or $Graph.binding.supervisor_session_id -cne 'graph-supervisor' -or
        $Graph.binding.binding_revision -ne 1 -or $Graph.snapshot_sha256 -cnotmatch '^[0-9a-f]{64}$' -or
        $Graph.commit_hash -cne $Graph.objects.commit.address -or $Graph.manifest_hash -cne $Graph.objects.manifest.address) {
        throw 'The Rust process receipt did not identify the expected revision and canonical graph links.'
    }
    foreach ($kind in @('ticket', 'header', 'manifest', 'commit')) {
        $record = $Graph.objects.$kind
        if ($record.address -cnotmatch '^[0-9a-f]{64}$' -or $record.link_count -ne 1 -or
            $record.identity.volume_serial -cne $Volume -or
            -not (Test-TicketRustWriterFileIdentity $record.identity $record.identity)) {
            throw "The Rust process receipt did not attest the physical $kind record."
        }
    }
}

function Assert-TicketRustGraphProcessMatch($Observed, $Published) {
    if ($Observed.commit_hash -cne $Published.commit_hash -or $Observed.manifest_hash -cne $Published.manifest_hash -or
        $Observed.snapshot_sha256 -cne $Published.snapshot_sha256) {
        throw 'The independently reopened Rust Ticket graph differed from the published revision.'
    }
    foreach ($kind in @('ticket', 'header', 'manifest', 'commit')) {
        $found = $Observed.objects.$kind
        $expected = $Published.objects.$kind
        if ($found.address -cne $expected.address -or
            -not (Test-TicketRustWriterFileIdentity $found.identity $expected.identity)) {
            throw "The independently reopened Rust Ticket $kind differed from the published record."
        }
    }
}

function Assert-TicketRustGraphProcessToken($ActorToken, $OwnerToken) {
    if ($ActorToken.sid -cne $OwnerToken.sid -or $ActorToken.elevated -ne $false -or
        $ActorToken.integrity_sid -cne 'S-1-16-8192' -or $ActorToken.administrators_group_present -ne $false -or
        $ActorToken.local_account_group_present -ne $true -or @($ActorToken.excess_privileges_present).Count -ne 0) {
        throw 'The Rust Ticket process did not attest the same ordinary local account.'
    }
}

function Assert-RustGraphProcessExitProbe($Receipt, [int]$OwnerProcessId) {
    $names = @('complete', 'before-head-replace', 'after-head-replace', 'after-scope-flush')
    $proof = $Receipt.process_exit
    if ($Receipt.probe -cne 'rust-graph-process-exit' -or $Receipt.case -cne 'native_standard_user_ticket_graph_process_exit' -or
        $Receipt.owner_pid -ne $OwnerProcessId -or $Receipt.verdict -cne 'available' -or $proof.qualified -ne $true -or
        @($proof.cases.PSObject.Properties).Count -ne $names.Count) {
        throw 'The Rust receipt did not identify the exact writer process-exit test and four cases.'
    }
    $volume = $Receipt.volume.volume_serial
    $scopes = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $locks = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    $publish = 'open_scope|acquire_writer|initial.ticket.category_flush|initial.header.category_flush|initial.manifest.category_flush|initial.commit.category_flush|initial.head.scope_flush|successor.ticket.category_flush|successor.header.category_flush|successor.manifest.category_flush|successor.commit.category_flush|head.stage_file_flush'
    $reopen = 'open_scope|acquire_writer|read_head|selected.commit|selected.manifest|selected.header|selected.ticket'
    foreach ($name in $names) {
        $trial = $proof.cases.$name
        $writer = $trial.writer
        $verifier = $trial.verifier
        $complete = $name -ceq 'complete'
        $renamed = $name -cne 'before-head-replace'
        $flushed = $complete -or $name -ceq 'after-scope-flush'
        $expectedState = if ($complete) { 'complete' } else { 'cut_reached' }
        $expectedExit = if ($complete) { 0 } else { 16980 }
        $expectedTermination = if ($complete) { $null } else { 0 }
        $expectedHeld = if ($complete) { $null } else { 33 }
        $expectedFlush = if ($flushed) { 0 } else { $null }
        if ($writer.phase -cne $name -or $writer.state -cne $expectedState -or
            $trial.writer_exit_code -ne $expectedExit -or $trial.termination_win32_error -ne $expectedTermination -or
            $trial.held_lock_win32_error -ne $expectedHeld -or $writer.scope_flush_win32_error -ne $expectedFlush -or
            $writer.pid -le 0 -or $verifier.pid -le 0 -or $writer.pid -eq $verifier.pid -or
            $writer.pid -eq $OwnerProcessId -or $verifier.pid -eq $OwnerProcessId -or
            $writer.scope.volume_serial -cne $volume -or $writer.writer.file.volume_serial -cne $volume -or
            $writer.scope.file_id -ceq $Receipt.volume.fixture_directory_file_id -or
            -not (Test-TicketRustWriterFileIdentity $verifier.scope $writer.scope) -or
            -not (Test-TicketRustWriterFileIdentity $verifier.writer.file $writer.writer.file) -or
            $writer.writer.link_count -ne 1 -or $verifier.writer.link_count -ne 1 -or
            $writer.writer.win32_error -ne 0 -or $verifier.writer.win32_error -ne 0 -or $verifier.lock_attempts -ne 1 -or
            -not $scopes.Add($writer.scope.file_id) -or -not $locks.Add($writer.writer.file.file_id)) {
            throw "The Rust Ticket '$name' did not attest the actual writer exit and fresh same-lock owner."
        }
        Assert-TicketRustGraphProcessToken $writer.token $Receipt.token
        Assert-TicketRustGraphProcessToken $verifier.token $Receipt.token

        $publishOrder = $publish
        if ($renamed) { $publishOrder += '|head.replace' }
        if ($flushed) { $publishOrder += '|head.scope_flush' }
        if ($complete) { $publishOrder += '|head.read_named' }
        if (($writer.operation_order -join '|') -cne $publishOrder) {
            throw "The Rust Ticket '$name' did not stop at its native publication boundary."
        }
        if ($renamed) {
            if ($writer.head_rename.ntstatus -cne '00000000' -or $writer.head_rename.win32_error -ne 0 -or
                $writer.head_rename.information_class -ne 65 -or $writer.head_rename.flags -ne 3) {
                throw "The Rust Ticket '$name' lacked the successful class 65 replacement."
            }
        } elseif ($null -ne $writer.head_rename) {
            throw 'The pre-replacement Rust cut reported a native rename.'
        }
        Assert-TicketRustGraphProcessRevision $writer.initial $volume 1 $null
        Assert-TicketRustGraphProcessRevision $writer.successor $volume 2 $writer.initial.commit_hash
        $oldBytes = [Convert]::ToHexString([Text.Encoding]::ASCII.GetBytes($writer.initial.commit_hash)).ToLowerInvariant()
        $newBytes = [Convert]::ToHexString([Text.Encoding]::ASCII.GetBytes($writer.successor.commit_hash)).ToLowerInvariant()
        if ($writer.old_head.bytes_hex -cne $oldBytes -or $writer.head_stage.bytes_hex -cne $newBytes -or
            $writer.old_head.identity.volume_serial -cne $volume -or $writer.head_stage.identity.volume_serial -cne $volume -or
            $writer.old_head.identity.file_id -ceq $writer.head_stage.identity.file_id -or
            -not (Test-TicketRustObjectObservation $writer.old_head $writer.old_head) -or
            -not (Test-TicketRustObjectObservation $writer.head_stage $writer.head_stage)) {
            throw "The Rust Ticket '$name' lacked distinct physical HEAD and staged successor witnesses."
        }
        if ($renamed) {
            Assert-TicketRustGraphProcessRevision $verifier.selected $volume 2 $writer.initial.commit_hash
            Assert-TicketRustGraphProcessRevision $verifier.previous $volume 1 $null
            Assert-TicketRustGraphProcessMatch $verifier.selected $writer.successor
            Assert-TicketRustGraphProcessMatch $verifier.previous $writer.initial
            if (($verifier.operation_order -join '|') -cne "$reopen|previous.commit|previous.manifest|previous.header|previous.ticket|stage_absent" -or
                -not (Test-TicketRustObjectObservation $verifier.head $writer.head_stage) -or
                $verifier.stage_absence_win32_error -ne 2 -or $null -ne $verifier.stage -or $null -ne $verifier.staged_graph) {
                throw "The Rust Ticket '$name' did not reopen the successor and prove its former stage absent."
            }
        } else {
            Assert-TicketRustGraphProcessRevision $verifier.selected $volume 1 $null
            Assert-TicketRustGraphProcessRevision $verifier.staged_graph $volume 2 $writer.initial.commit_hash
            Assert-TicketRustGraphProcessMatch $verifier.selected $writer.initial
            Assert-TicketRustGraphProcessMatch $verifier.staged_graph $writer.successor
            if (($verifier.operation_order -join '|') -cne "$reopen|read_stage|staged.commit|staged.manifest|staged.header|staged.ticket" -or
                -not (Test-TicketRustObjectObservation $verifier.head $writer.old_head) -or
                -not (Test-TicketRustObjectObservation $verifier.stage $writer.head_stage) -or
                $null -ne $verifier.stage_absence_win32_error -or $null -ne $verifier.previous) {
                throw 'The Rust Ticket pre-replacement cut did not retain the initial graph and intact stage.'
            }
        }
    }
    Assert-TicketRustFileStoreGate $Receipt
}
