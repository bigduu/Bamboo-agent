public static partial class WindowsTicketDirectoryFlushProbe
{
    private const string JunctionSource = "junction-source", JunctionTarget = "junction-target", JunctionBridge = "bridge";
    private const uint MountPointTag = 0xa0000003, GetReparsePoint = 0x000900a8;
    private const int ReparseEncountered = 4395;
    private const string ReparseStatus = "c000050b";

    [DllImport("kernel32.dll", ExactSpelling = true, SetLastError = true)]
    private static extern bool DeviceIoControl(SafeFileHandle file, uint control, IntPtr input, uint inputLength,
        [Out] byte[] output, uint outputLength, out uint returned, IntPtr overlapped);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true, SetLastError = true)]
    private static extern bool CreateHardLinkW(string newName, string existingName, IntPtr security);

    public static void ValidateScopeIdentityFixture(string expectedSid, string fixtureDirectory)
    {
        using var scope = OpenWriterScope(expectedSid, fixtureDirectory, requireEmpty: true);
    }

    public static Dictionary<string, object> RunScopeIdentity(string expectedSid, string fixtureDirectory)
    {
        using (WriterScope scope = OpenWriterScope(expectedSid, fixtureDirectory, requireEmpty: false))
            return scope.MeasureScopeIdentity(expectedSid);
    }

    public sealed partial class WriterScope
    {
        private void CreateIdentityDirectory(PublicationPhase phase, string name)
        {
            using (var directory = OpenPublicationObject(phase, "directory_create", Folder, name,
                GenericWrite | WriterDirectoryAccess, WriterShare, NtCreate, directory: true))
                phase.Receipt["directory_file_id"] = PublicationIdentity(directory, Path.Combine(path, name), true).Id;
        }

        private string WriteIdentityWitness(PublicationPhase phase, string name, byte[] bytes)
        {
            using (var file = OpenPublicationObject(phase, "witness_create", Folder, name,
                GenericWrite | ReadAttributes | WriterSynchronize, PublicationShare, NtCreate, directory: false))
                return WriteStage(phase, file, Path.Combine(path, name), bytes);
        }

        private string[] ReadIdentityEntries(PublicationPhase phase)
        {
            VerifyFolder();
            phase.Receipt["scope_file_id"] = folderId;
            phase.Receipt["volume_serial"] = volumeSerial;
            var names = new List<string>();
            phase.Record("entries", names);
            foreach (string entry in Directory.EnumerateFileSystemEntries(path))
            {
                if (names.Count == 16) throw new IOException("The fixture has too many direct directory entries.");
                names.Add(Path.GetFileName(entry));
            }
            names.Sort(StringComparer.Ordinal);
            return names.ToArray();
        }

        private bool ObserveIdentityWitness(PublicationPhase phase, string name, string expectedId, byte[] bytes,
            string[] expectedEntries)
        {
            string[] entries = ReadIdentityEntries(phase);
            bool matched = ReadNamed(phase, "witness", Folder, path, name, expectedId, bytes);
            phase.Qualified = matched && entries.AsSpan().SequenceEqual(expectedEntries);
            return phase.Qualified;
        }

        private void MeasureIdentityPositive(PublicationPhase phase, string expectedSid)
        {
            CreateIdentityDirectory(phase, "good");
            using (var good = OpenWriterScope(expectedSid, Path.Combine(path, "good"), requireEmpty: true))
            using (var locked = good.OpenLock(create: true))
            {
                var acquired = locked.TryLock();
                phase.Record("acquisition", acquired);
                phase.Receipt["scope_file_id"] = good.folderId;
                phase.Receipt["file_id"] = locked.FileId;
                phase.Receipt["volume_serial"] = locked.VolumeSerial;
                string namedId = good.NamedId(WriterName);
                phase.Record("named_file_id", namedId);
                phase.Qualified = (bool)acquired["succeeded"] && namedId == locked.FileId && locked.VolumeSerial == volumeSerial;
            }
        }

        private static int NativeOutcome(bool succeeded, string operation) => succeeded ? 0 : FailureCode(operation);

        private static void RequireIdentityCase(bool condition, string message)
        {
            if (!condition) throw new IOException(message);
        }

        private string ObserveBridge(PublicationPhase phase, WriterScope source, WriterScope target)
        {
            string link = Path.Combine(source.path, JunctionBridge);
            using (var bridge = Open(link, GenericRead | ReadAttributes, PublicationShare, OpenExisting, BackupSemantics))
            {
                int openError = NativeOutcome(!bridge.IsInvalid, "Open the junction itself");
                phase.Record("open", Operation(openError));
                if (openError != 0) throw WindowsError("Open the junction itself", openError);

                bool attrRead = ReadFileAttributes(bridge, FileAttributeTagInfo, out AttributeTag attributes,
                    (uint)Marshal.SizeOf<AttributeTag>());
                int attrError = NativeOutcome(attrRead, "Read junction attributes");
                phase.Record("attributes_read", Operation(attrError));
                if (attrError != 0) throw WindowsError("Read junction attributes", attrError);
                phase.Receipt["tag"] = attributes.ReparseTag.ToString("x8");
                RequireIdentityCase((attributes.Attributes & (ReparseAttribute | DirectoryAttribute)) ==
                    (ReparseAttribute | DirectoryAttribute) && attributes.ReparseTag == MountPointTag,
                    "The bridge is not a native directory junction.");

                bool idRead = ReadFileIdentity(bridge, FileIdInfo, out FileIdentity identity, (uint)Marshal.SizeOf<FileIdentity>());
                int idError = NativeOutcome(idRead, "Read junction file identity");
                phase.Record("identity_read", Operation(idError));
                if (idError != 0) throw WindowsError("Read junction file identity", idError);
                phase.Receipt["file_id"] = identity.Id;
                phase.Receipt["volume_serial"] = identity.Volume;
                phase.Receipt["target_directory_file_id"] = target.folderId;
                RequireIdentityCase(identity.Volume == volumeSerial && target.volumeSerial == volumeSerial && identity.Id != target.folderId,
                    "The bridge handle did not identify the junction itself on the target volume.");

                // The no-follow handle returns the junction's own mount-point buffer, without traversing it.
                byte[] buffer = new byte[16 * 1024];
                bool queried = DeviceIoControl(bridge, GetReparsePoint, IntPtr.Zero, 0, buffer, (uint)buffer.Length,
                    out uint count, IntPtr.Zero);
                int queryError = NativeOutcome(queried, "Read the junction reparse data");
                var query = Operation(queryError);
                query["bytes_returned"] = count;
                phase.Record("reparse_query", query);
                if (queryError != 0) throw WindowsError("Read the junction reparse data", queryError);
                RequireIdentityCase(count >= 16 && count <= buffer.Length && BitConverter.ToUInt32(buffer, 0) == MountPointTag,
                    "The junction reparse data has an invalid header.");
                int end = 8 + BitConverter.ToUInt16(buffer, 4);
                int offset = BitConverter.ToUInt16(buffer, 8);
                int length = BitConverter.ToUInt16(buffer, 10);
                RequireIdentityCase(end <= count && length > 0 && (length & 1) == 0 && (offset & 1) == 0 && 16 + offset + length <= end,
                    "The junction reparse substitute name is malformed.");
                string targetName = Encoding.Unicode.GetString(buffer, 16 + offset, length);
                phase.Receipt["substitute_name"] = targetName;
                phase.Qualified = String.Equals(targetName.TrimEnd('\\'), @"\??\" + target.path, StringComparison.OrdinalIgnoreCase);
                RequireIdentityCase(phase.Qualified, "The junction does not point to its exact owned sibling target.");
                return identity.Id;
            }
        }

        private static Dictionary<string, object> RecordScopeRejection(PublicationPhase phase, Func<IDisposable> open)
        {
            var receipt = new Dictionary<string, object>
            {
                ["attempted"] = true, ["handle_returned"] = false, ["lock_attempted"] = false,
                ["win32_error"] = null, ["ntstatus"] = null, ["qualified"] = false
            };
            phase.Record("rejection", receipt);
            try
            {
                using (var handle = open()) receipt["handle_returned"] = true;
            }
            catch (Win32Exception error)
            {
                receipt["error_kind"] = nameof(Win32Exception);
                receipt["error"] = error.Message;
                receipt["win32_error"] = error.NativeErrorCode;
                const string statusPrefix = "(NTSTATUS 0x";
                int marker = error.Message.IndexOf(statusPrefix, StringComparison.Ordinal);
                if (marker >= 0 && marker + statusPrefix.Length + 8 < error.Message.Length)
                    receipt["ntstatus"] = error.Message.Substring(marker + statusPrefix.Length, 8);
            }
            catch (IOException error)
            {
                receipt["error_kind"] = nameof(IOException);
                receipt["error"] = error.Message;
            }
            catch (InvalidOperationException error)
            {
                receipt["error_kind"] = nameof(InvalidOperationException);
                receipt["error"] = error.Message;
            }
            return receipt;
        }

        private void MeasureIdentityJunction(PublicationPhase phase, string expectedSid)
        {
            using (var source = OpenWriterScope(expectedSid, Path.Combine(path, JunctionSource), requireEmpty: false))
            using (var target = OpenWriterScope(expectedSid, Path.Combine(path, JunctionTarget), requireEmpty: false))
            using (var child = OpenWriterScope(expectedSid, Path.Combine(target.path, "child"), requireEmpty: true))
            {
                const string sourceWitness = "source-witness.bin", targetWitness = "target-witness.bin";
                byte[] sourceBytes = Encoding.ASCII.GetBytes("junction source witness\n");
                byte[] targetBytes = Encoding.ASCII.GetBytes("junction target witness\n");
                string sourceId = source.WriteIdentityWitness(phase.Child("source_setup"), sourceWitness, sourceBytes);
                string targetId = child.WriteIdentityWitness(phase.Child("target_setup"), targetWitness, targetBytes);

                using (var direct = OpenWriterScope(expectedSid, child.path, requireEmpty: false))
                {
                    bool same = direct.folderId == child.folderId && direct.volumeSerial == volumeSerial;
                    phase.Record("direct_target", new Dictionary<string, object>
                    {
                        ["qualified"] = same, ["scope_file_id"] = direct.folderId, ["volume_serial"] = direct.volumeSerial
                    });
                    RequireIdentityCase(same, "The direct positive control did not reopen the target scope.");
                }

                var before = phase.Child("before");
                string originalBridge = ObserveBridge(before.Child("bridge"), source, target);
                bool sourceBefore = source.ObserveIdentityWitness(before.Child("source"), sourceWitness, sourceId, sourceBytes,
                    new[] { JunctionBridge, sourceWitness });
                bool childBefore = child.ObserveIdentityWitness(before.Child("target_child"), targetWitness, targetId, targetBytes,
                    new[] { targetWitness });
                bool targetBefore = target.ReadIdentityEntries(before.Child("target")).AsSpan().SequenceEqual(new[] { "child" });
                before.Qualified = sourceBefore && targetBefore && childBefore;
                RequireIdentityCase(before.Qualified, "The junction witness scope has unexpected entries or bytes.");

                string bridgePath = Path.Combine(source.path, JunctionBridge);
                string junctionScope = Path.Combine(bridgePath, "child");
                var rejected = RecordScopeRejection(phase, () => OpenWriterScope(expectedSid, junctionScope, requireEmpty: false));
                rejected.TryGetValue("error_kind", out object kind);
                rejected.TryGetValue("error", out object message);
                bool nativeReparse = kind is string nativeKind && nativeKind == nameof(Win32Exception) &&
                    rejected["win32_error"] is int win32 && win32 == ReparseEncountered && (string)rejected["ntstatus"] == ReparseStatus &&
                    ((string)message).StartsWith("Open a writer-scope directory (NTSTATUS 0x", StringComparison.Ordinal);
                bool foundReparseHandle = kind is string validationKind && validationKind == nameof(InvalidOperationException) &&
                    (string)message == $"Unexpected object type or reparse point: {bridgePath}";
                bool qualifiedRejection = !(bool)rejected["handle_returned"] && (nativeReparse || foundReparseHandle);
                rejected["qualified"] = qualifiedRejection;
                rejected["stage"] = nativeReparse ? "ancestry-open" : foundReparseHandle ? "ancestor-reparse-attributes" : null;

                var after = phase.Child("after");
                string finalBridge = ObserveBridge(after.Child("bridge"), source, target);
                bool sourceAfter = source.ObserveIdentityWitness(after.Child("source"), sourceWitness, sourceId, sourceBytes,
                    new[] { JunctionBridge, sourceWitness });
                bool childAfter = child.ObserveIdentityWitness(after.Child("target_child"), targetWitness, targetId, targetBytes,
                    new[] { targetWitness });
                bool targetAfter = target.ReadIdentityEntries(after.Child("target")).AsSpan().SequenceEqual(new[] { "child" });
                after.Qualified = sourceAfter && targetAfter && childAfter && originalBridge == finalBridge;
                phase.Qualified = qualifiedRejection && after.Qualified;
            }
        }

        private bool ObserveLinkWitness(PublicationPhase phase, string operation, string name, string expectedId, byte[] bytes, uint links)
        {
            using (var file = OpenPublicationObject(phase, operation + "_open", Folder, name,
                GenericRead | WriterSynchronize, PublicationShare, NtOpen, directory: false))
            {
                var observed = new Dictionary<string, object>();
                phase.Record(operation, observed);
                // Use the relative open and raw handle metadata to observe the deliberately invalid link count.
                observed["file_type"] = GetFileType(file);
                bool attrs = ReadFileAttributes(file, FileAttributeTagInfo, out AttributeTag tag,
                    (uint)Marshal.SizeOf<AttributeTag>());
                int attrError = NativeOutcome(attrs, "Read the lock witness attributes");
                observed["attributes_read"] = Operation(attrError);
                if (attrError != 0) throw WindowsError("Read the lock witness attributes", attrError);
                observed["attributes"] = tag.Attributes;
                bool standard = ReadWriterStandardInfo(file, StandardFileInfo, out WriterStandardInfo info,
                    (uint)Marshal.SizeOf<WriterStandardInfo>());
                int standardError = NativeOutcome(standard, "Read the lock witness link count");
                observed["standard_info"] = Operation(standardError);
                if (standardError != 0) throw WindowsError("Read the lock witness link count", standardError);
                observed["link_count"] = info.NumberOfLinks;
                observed["delete_pending"] = info.DeletePending != 0;
                observed["directory"] = info.Directory != 0;
                byte[] actual = ReadPublicationBytes(file, observed, bytes.Length);
                bool matched = MatchPublicationRead(actual, observed, expectedId, bytes);
                return (uint)observed["file_type"] == FileTypeDisk &&
                    (tag.Attributes & (ReparseAttribute | DirectoryAttribute)) == 0 &&
                    info.NumberOfLinks == links && info.DeletePending == 0 && info.Directory == 0 && matched;
            }
        }

        private void MeasureIdentityMultilink(PublicationPhase phase, string expectedSid)
        {
            const string folder = "hardlink", alias = "writer-alias.lock";
            byte[] witness = Encoding.ASCII.GetBytes("writer lock link-count witness\n");
            CreateIdentityDirectory(phase, folder);
            using (var scope = OpenWriterScope(expectedSid, Path.Combine(path, folder), requireEmpty: true))
            {
                string originalId = scope.WriteIdentityWitness(phase.Child("setup"), WriterName, witness);
                var baseline = phase.Child("baseline");
                bool single = scope.ObserveLinkWitness(baseline, "original", WriterName, originalId, witness, 1);
                using (var locked = scope.OpenLock(create: false))
                {
                    var acquired = locked.TryLock();
                    baseline.Record("acquisition", acquired);
                    baseline.Receipt["file_id"] = locked.FileId;
                    baseline.Receipt["volume_serial"] = locked.VolumeSerial;
                    baseline.Receipt["scope_file_id"] = scope.folderId;
                    baseline.Qualified = single && (bool)acquired["succeeded"] && locked.FileId == originalId &&
                        locked.VolumeSerial == volumeSerial;
                }
                RequireIdentityCase(baseline.Qualified, "The same single-link lock did not pass its positive control.");

                bool linked = CreateHardLinkW(ExtendedPathPrefix + Path.Combine(scope.path, alias),
                    ExtendedPathPrefix + Path.Combine(scope.path, WriterName), IntPtr.Zero);
                int linkError = NativeOutcome(linked, "Create the same-volume lock alias");
                phase.Record("create_hard_link", Operation(linkError));
                if (linkError != 0) throw WindowsError("Create the same-volume lock alias", linkError);

                var before = phase.Child("before");
                bool originalBefore = scope.ObserveLinkWitness(before, "original", WriterName, originalId, witness, 2);
                bool aliasBefore = scope.ObserveLinkWitness(before, "alias", alias, originalId, witness, 2);
                bool namesBefore = scope.ReadIdentityEntries(before).AsSpan().SequenceEqual(new[] { alias, WriterName });
                before.Qualified = originalBefore && aliasBefore && namesBefore;
                RequireIdentityCase(before.Qualified, "The real hard-link control did not retain both names, the link count and witness bytes.");

                var rejected = RecordScopeRejection(phase, () => scope.OpenLock(create: false));
                rejected.TryGetValue("error_kind", out object kind);
                rejected.TryGetValue("error", out object message);
                bool qualified = !(bool)rejected["handle_returned"] && (string)kind == nameof(IOException) &&
                    (string)message == "The probe requires a single-link regular file with no pending deletion.";
                rejected["stage"] = qualified ? "single-link-validation" : null;
                rejected["qualified"] = qualified;

                var after = phase.Child("after");
                bool originalAfter = scope.ObserveLinkWitness(after, "original", WriterName, originalId, witness, 2);
                bool aliasAfter = scope.ObserveLinkWitness(after, "alias", alias, originalId, witness, 2);
                bool namesAfter = scope.ReadIdentityEntries(after).AsSpan().SequenceEqual(new[] { alias, WriterName });
                after.Qualified = originalAfter && aliasAfter && namesAfter;
                phase.Qualified = qualified && after.Qualified;
            }
        }

        internal Dictionary<string, object> MeasureScopeIdentity(string expectedSid)
        {
            var positive = new PublicationPhase();
            var junction = new PublicationPhase();
            var multilink = new PublicationPhase();
            positive.Measure(() => MeasureIdentityPositive(positive, expectedSid));
            junction.Measure(() => MeasureIdentityJunction(junction, expectedSid));
            multilink.Measure(() => MeasureIdentityMultilink(multilink, expectedSid));
            bool qualified = positive.Qualified && junction.Qualified && multilink.Qualified;
            return new Dictionary<string, object>
            {
                ["schema"] = 1,
                ["probe"] = "scope-identity",
                ["windows_version"] = Environment.OSVersion.Version.ToString(),
                ["process_architecture"] = RuntimeInformation.ProcessArchitecture.ToString(),
                ["token"] = Token,
                ["volume"] = Volume,
                ["ordered_crash_persistence_proven"] = false,
                ["verdict"] = qualified ? "available" : "unavailable",
                ["scope_identity_qualified"] = qualified,
                ["scope_identity"] = new Dictionary<string, object>
                {
                    ["positive"] = positive.Receipt, ["junction"] = junction.Receipt, ["multilink"] = multilink.Receipt
                }
            };
        }
    }
}
