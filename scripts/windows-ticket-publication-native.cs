public static partial class WindowsTicketDirectoryFlushProbe
{
    private const string PublicationHead = "HEAD";
    private const uint PublicationShare = WriterShare | WriterShareDelete;
    private const int PublicationNameCollision = unchecked((int)0xc0000035);
    private const int PublicationAccessDenied = unchecked((int)0xc0000022);
    private const int PublicationSharingViolation = unchecked((int)0xc0000043);

    [DllImport("kernel32.dll", ExactSpelling = true, SetLastError = true)]
    private static extern bool SetFilePointerEx(SafeFileHandle file, long offset, out long position, uint origin);

    [DllImport("kernel32.dll", ExactSpelling = true, SetLastError = true)]
    private static extern bool ReadFile(SafeFileHandle file, [Out] byte[] bytes, uint count,
        out uint read, IntPtr overlapped);

    public static Dictionary<string, object> RunPublication(string expectedSid, string fixtureDirectory)
    {
        using (WriterScope scope = OpenWriterScope(expectedSid, fixtureDirectory, requireEmpty: true))
            return scope.MeasurePublication();
    }

    private sealed class PublicationPhase
    {
        private readonly List<string> order;
        private readonly string prefix;
        public Dictionary<string, object> Receipt { get; } = new Dictionary<string, object>();
        public bool Qualified
        {
            get => (bool)Receipt["qualified"];
            set => Receipt["qualified"] = value;
        }

        public PublicationPhase()
        {
            order = new List<string>();
            prefix = "";
            Receipt["operation_order"] = order;
            Qualified = false;
        }

        private PublicationPhase(List<string> order, string prefix)
        {
            this.order = order;
            this.prefix = prefix;
            Qualified = false;
        }

        public PublicationPhase Child(string name)
        {
            var phase = new PublicationPhase(order, prefix + name + ".");
            Receipt[name] = phase.Receipt;
            return phase;
        }

        public void Record(string operation, object result)
        {
            order.Add(prefix + operation);
            Receipt[operation] = result;
        }

        public void Measure(Action trial)
        {
            try { trial(); }
            catch (Win32Exception error)
            {
                Qualified = false;
                Receipt["error"] = error.Message;
                Receipt["win32_error"] = error.NativeErrorCode;
            }
            catch (IOException error)
            {
                Qualified = false;
                Receipt["error"] = error.Message;
            }
            catch (InvalidOperationException error)
            {
                Qualified = false;
                Receipt["error"] = error.Message;
            }
        }
    }

    public sealed partial class WriterScope
    {
        private static string Hex(byte[] bytes) => Convert.ToHexString(bytes).ToLowerInvariant();

        private static string ContentName(byte[] bytes) => Hex(System.Security.Cryptography.SHA256.HashData(bytes));

        private SafeFileHandle OpenPublicationObject(PublicationPhase phase, string operation,
            SafeFileHandle parent, string name, uint access, uint share, uint disposition, bool directory)
        {
            SafeFileHandle handle = OpenWriterRelative(parent, name, access, share, disposition, directory, out int status);
            var receipt = RenameResult(status, disposition == NtCreate ? "create" : "open");
            receipt["name"] = name;
            receipt["access"] = $"0x{access:x8}";
            receipt["share"] = $"0x{share:x8}";
            phase.Record(operation, receipt);
            if (handle == null) throw WriterNtError($"{operation} {name}", status);
            return handle;
        }

        private FileIdentity PublicationIdentity(SafeFileHandle handle, string fullName, bool directory)
        {
            FileIdentity identity = directory ? InspectName(handle, fullName, true) : InspectWriterFile(handle, fullName);
            if (identity.Volume != volumeSerial) throw new IOException("The publication object is on another volume.");
            return identity;
        }

        private string WriteStage(PublicationPhase phase, SafeFileHandle file, string fullName, byte[] content)
        {
            string id = PublicationIdentity(file, fullName, false).Id;
            phase.Receipt["stage_file_id"] = id;
            phase.Receipt["expected_bytes_hex"] = Hex(content);
            bool succeeded = WriteFile(file, content, (uint)content.Length, out uint count, IntPtr.Zero);
            int writeError = succeeded ? 0 : FailureCode("Write the publication stage");
            var write = Operation(writeError);
            write["bytes_written"] = count;
            phase.Record("stage_write", write);
            if (writeError != 0) throw WindowsError("Write the publication stage", writeError);
            if (count != content.Length) throw new IOException("The publication stage write was incomplete.");
            int flushError = Flush(file);
            phase.Record("stage_flush", Operation(flushError));
            if (flushError != 0) throw WindowsError("Flush the publication stage", flushError);
            return id;
        }

        private bool FlushPublicationDirectory(PublicationPhase phase, string operation, SafeFileHandle directory,
            string fullName, string expectedId)
        {
            FileIdentity identity = PublicationIdentity(directory, fullName, true);
            if (identity.Id != expectedId) throw new IOException("The publication directory identity changed.");
            int error = Flush(directory);
            var receipt = Operation(error);
            receipt["file_id"] = identity.Id;
            receipt["volume_serial"] = identity.Volume;
            phase.Record(operation, receipt);
            return error == 0;
        }

        private bool ReadHeld(SafeFileHandle file, Dictionary<string, object> receipt,
            string expectedId, byte[] expected)
        {
            // Replaced old readers may have no current name. FileIdInfo and I/O work through the handle.
            bool identityRead = ReadFileIdentity(file, FileIdInfo, out FileIdentity identity,
                (uint)Marshal.SizeOf<FileIdentity>());
            int identityError = identityRead ? 0 : FailureCode("Read the publication file ID");
            receipt["identity_read"] = Operation(identityError);
            if (identityRead)
            {
                receipt["file_id"] = identity.Id;
                receipt["volume_serial"] = identity.Volume;
            }
            bool seek = SetFilePointerEx(file, 0, out long position, 0);
            int seekError = seek ? 0 : FailureCode("Seek the publication file");
            receipt["seek"] = Operation(seekError);
            if (!seek || position != 0) return false;

            var buffer = new byte[expected.Length + 1];
            bool read = ReadFile(file, buffer, (uint)buffer.Length, out uint count, IntPtr.Zero);
            int readError = read ? 0 : FailureCode("Read the publication file");
            receipt["read"] = Operation(readError);
            if (!read || count > buffer.Length) return false;
            receipt["bytes_hex"] = Convert.ToHexString(buffer.AsSpan(0, (int)count)).ToLowerInvariant();
            bool exact = identityRead && identity.Id == expectedId && identity.Volume == volumeSerial &&
                buffer.AsSpan(0, (int)count).SequenceEqual(expected);
            receipt["matched"] = exact;
            return exact;
        }

        private bool ReadHeld(PublicationPhase phase, string operation, SafeFileHandle file,
            string expectedId, byte[] expected)
        {
            var receipt = new Dictionary<string, object>();
            phase.Record(operation, receipt);
            return ReadHeld(file, receipt, expectedId, expected);
        }

        private bool ReadNamed(PublicationPhase phase, string operation, SafeFileHandle directory,
            string dirPath, string name, string expectedId, byte[] expected)
        {
            using (var handle = OpenWriterRelative(directory, name, GenericRead | WriterSynchronize,
                PublicationShare, NtOpen, directory: false, out int status))
            {
                var receipt = new Dictionary<string, object> { ["open"] = RenameResult(status, "open"), ["name"] = name };
                phase.Record(operation, receipt);
                if (handle == null) return false;
                PublicationIdentity(handle, Path.Combine(dirPath, name), false);
                return ReadHeld(handle, receipt, expectedId, expected);
            }
        }

        private static bool CheckAbsent(PublicationPhase phase, SafeFileHandle directory, string name)
        {
            using (var handle = OpenWriterRelative(directory, name, ReadAttributes | WriterSynchronize,
                PublicationShare, NtOpen, directory: false, out int status))
            {
                var receipt = RenameResult(status, "open");
                bool absent = handle == null && (status == ObjectNameNotFound || status == NoSuchFile);
                receipt["absent"] = absent;
                receipt["name"] = name;
                phase.Record("stage_absence", receipt);
                return absent;
            }
        }

        private static Dictionary<string, object> PublicationRename(PublicationPhase phase, SafeFileHandle source,
            SafeFileHandle directory, string name, uint flags)
        {
            var rename = RenameOpened(source, directory, name, flags);
            rename["destination"] = name;
            rename["information_class"] = (flags & RenamePosix) == 0 ? RenameInfo : RenameInfoEx;
            rename["flags"] = flags;
            phase.Record("rename", rename);
            return rename;
        }

        private string InstallNewName(PublicationPhase phase, SafeFileHandle directory, string dirPath,
            string dirId, string stageName, string name, byte[] content)
        {
            using (var stage = OpenPublicationObject(phase, "stage_create", directory, stageName,
                GenericWrite | GenericRead | WriterDelete | WriterSynchronize, PublicationShare, NtCreate, directory: false))
            {
                string stagedId = WriteStage(phase, stage, Path.Combine(dirPath, stageName), content);
                var rename = PublicationRename(phase, stage, directory, name, 0);
                bool renamed = (bool)rename["succeeded"];
                bool named = ReadNamed(phase, "named_file", directory, dirPath, name, stagedId, content);
                bool absent = CheckAbsent(phase, directory, stageName);
                phase.Qualified = renamed && named && absent &&
                    FlushPublicationDirectory(phase, "directory_flush", directory, dirPath, dirId);
                return stagedId;
            }
        }

        private bool ReuseObject(PublicationPhase phase, string observation, SafeFileHandle directory,
            string dirPath, string dirId, string name, string expectedId, byte[] expected)
        {
            bool matched = ReadNamed(phase, observation, directory, dirPath, name, expectedId, expected);
            return matched && FlushPublicationDirectory(phase, "directory_flush", directory, dirPath, dirId);
        }

        private void MeasureImmutable(PublicationPhase immutable)
        {
            var insert = immutable.Child("insert");
            var collision = immutable.Child("collision");
            var reuse = immutable.Child("reuse");
            var conflict = immutable.Child("conflict");
            const string categoryName = "objects";
            string directoryPath = Path.Combine(path, categoryName);
            using (var category = OpenPublicationObject(immutable, "category_create", Folder, categoryName,
                GenericWrite | WriterDirectoryAccess, WriterShare, NtCreate, directory: true))
            {
                string directoryId = PublicationIdentity(category, directoryPath, true).Id;
                immutable.Receipt["category_file_id"] = directoryId;
                byte[] canonical = Encoding.ASCII.GetBytes("immutable publication object\n");
                byte[] different = Encoding.ASCII.GetBytes("different scratch object\n");
                string name = ContentName(canonical);
                string originalId = null;
                insert.Measure(() => originalId = InstallNewName(insert, category, directoryPath, directoryId,
                    "insert-stage", name, canonical));
                if (!insert.Qualified) return;

                collision.Measure(() =>
                {
                    const string stagedName = "collision-stage";
                    using (var stage = OpenPublicationObject(collision, "stage_create", category, stagedName,
                        GenericRead | GenericWrite | WriterDelete | WriterSynchronize, PublicationShare, NtCreate, directory: false))
                    {
                        string stagedId = WriteStage(collision, stage, Path.Combine(directoryPath, stagedName), different);
                        var rename = PublicationRename(collision, stage, category, name, 0);
                        bool originalPreserved = ReadNamed(collision, "original_file", category, directoryPath,
                            name, originalId, canonical);
                        bool stagePreserved = ReadNamed(collision, "staged_file", category, directoryPath,
                            stagedName, stagedId, different);
                        collision.Qualified = rename["ntstatus"].Equals(PublicationNameCollision.ToString("x8")) &&
                            originalPreserved && stagePreserved;
                    }
                });

                reuse.Measure(() => reuse.Qualified = ReuseObject(reuse, "existing_file", category,
                    directoryPath, directoryId, name, originalId, canonical));

                conflict.Measure(() =>
                {
                    // Model a damaged hash entry with actual wrong bytes at the expected name.
                    string conflictingName = ContentName(different);
                    string conflictId;
                    using (var bad = OpenPublicationObject(conflict, "stage_create", category, conflictingName,
                        GenericWrite | WriterSynchronize, PublicationShare, NtCreate, directory: false))
                        conflictId = WriteStage(conflict, bad, Path.Combine(directoryPath, conflictingName), canonical);
                    bool accepted = ReuseObject(conflict, "conflicting_file", category, directoryPath, directoryId,
                        conflictingName, conflictId, different);
                    var observed = (Dictionary<string, object>)conflict.Receipt["conflicting_file"];
                    bool denied = !accepted && observed.TryGetValue("bytes_hex", out object actual) &&
                        (string)actual == Hex(canonical) && observed.TryGetValue("file_id", out object fileId) &&
                        (string)fileId == conflictId && (string)observed["volume_serial"] == volumeSerial;
                    conflict.Receipt["expected_content_address"] = conflictingName;
                    conflict.Receipt["reuse_refused"] = denied;
                    conflict.Qualified = denied;
                });
                immutable.Qualified = insert.Qualified && collision.Qualified && reuse.Qualified && conflict.Qualified;
            }
        }

        private bool MeasureDeniedHead(PublicationPhase phase, string stageName, string oldId,
            byte[] oldContent, byte[] newContent, bool hasDelete, bool sharesDelete)
        {
            using (var old = OpenPublicationObject(phase, "old_reader_open", Folder, PublicationHead,
                GenericRead | WriterSynchronize, sharesDelete ? PublicationShare : WriterShare, NtOpen, directory: false))
            using (var stage = OpenPublicationObject(phase, "stage_create", Folder, stageName,
                GenericRead | GenericWrite | WriterSynchronize | (hasDelete ? WriterDelete : 0),
                PublicationShare, NtCreate, directory: false))
            {
                PublicationIdentity(old, Path.Combine(path, PublicationHead), false);
                bool oldBefore = ReadHeld(phase, "old_reader_before", old, oldId, oldContent);
                string stagedId = WriteStage(phase, stage, Path.Combine(path, stageName), newContent);
                if (!oldBefore || stagedId == oldId) return false;
                var rename = PublicationRename(phase, stage, Folder, PublicationHead, RenameReplace | RenamePosix);
                bool oldAfter = ReadHeld(phase, "old_reader_after", old, oldId, oldContent);
                bool namedOld = ReadNamed(phase, "named_head", Folder, path, PublicationHead, oldId, oldContent);
                bool namedStage = ReadNamed(phase, "named_stage", Folder, path, stageName, stagedId, newContent);
                bool preserved = oldAfter && namedOld && namedStage;
                string nativeStatus = (string)rename["ntstatus"];
                bool denied = nativeStatus == PublicationAccessDenied.ToString("x8") ||
                    (hasDelete && nativeStatus == PublicationSharingViolation.ToString("x8"));
                phase.Receipt["old_and_stage_preserved"] = preserved;
                phase.Qualified = preserved && denied;
                return preserved;
            }
        }

        private void MeasureHead(PublicationPhase head)
        {
            var initial = head.Child("initial");
            var noSourceDelete = head.Child("no_source_delete");
            var noReaderDeleteShare = head.Child("no_reader_delete_share");
            var replace = head.Child("replace");
            string scopeName = Path.GetFileName(path);
            using (var writable = OpenPublicationObject(head, "scope_write_open", ancestry[ancestry.Count - 2],
                scopeName, GenericWrite | WriterDirectoryAccess, WriterShare, NtOpen, directory: true))
            {
                if (PublicationIdentity(writable, path, true).Id != folderId)
                    throw new IOException("The writable scope handle does not name the retained scope.");
                byte[] oldContent = Encoding.ASCII.GetBytes(ContentName(Encoding.ASCII.GetBytes("old snapshot")) + "\n");
                byte[] newContent = Encoding.ASCII.GetBytes(ContentName(Encoding.ASCII.GetBytes("new snapshot")) + "\n");
                string oldId = null;
                initial.Measure(() => oldId = InstallNewName(initial, writable, path, folderId,
                    "head-initial-stage", PublicationHead, oldContent));
                if (!initial.Qualified) return;

                bool survivedSource = false;
                noSourceDelete.Measure(() => survivedSource = MeasureDeniedHead(noSourceDelete, "head-no-source-delete-stage",
                    oldId, oldContent, newContent, hasDelete: false, sharesDelete: true));
                if (!survivedSource) return;
                bool survivedReader = false;
                noReaderDeleteShare.Measure(() => survivedReader = MeasureDeniedHead(noReaderDeleteShare, "head-no-reader-share-stage",
                    oldId, oldContent, newContent, hasDelete: true, sharesDelete: false));
                if (!survivedReader) return;

                replace.Measure(() =>
                {
                    const string stageName = "head-positive-stage";
                    using (var old = OpenPublicationObject(replace, "old_reader_open", Folder, PublicationHead,
                        GenericRead | WriterSynchronize, PublicationShare, NtOpen, directory: false))
                    using (var stage = OpenPublicationObject(replace, "stage_create", Folder, stageName,
                        GenericRead | GenericWrite | WriterDelete | WriterSynchronize, PublicationShare, NtCreate, directory: false))
                    {
                        PublicationIdentity(old, Path.Combine(path, PublicationHead), false);
                        bool oldBefore = ReadHeld(replace, "old_reader_before", old, oldId, oldContent);
                        string stagedId = WriteStage(replace, stage, Path.Combine(path, stageName), newContent);
                        if (!oldBefore || stagedId == oldId) return;
                        var rename = PublicationRename(replace, stage, Folder, PublicationHead, RenameReplace | RenamePosix);
                        bool renamed = (bool)rename["succeeded"];
                        bool flushed = renamed && FlushPublicationDirectory(replace, "directory_flush", writable, path, folderId);
                        bool oldAfter = ReadHeld(replace, "old_reader_after", old, oldId, oldContent);
                        bool newNamed = ReadNamed(replace, "named_head", Folder, path, PublicationHead, stagedId, newContent);
                        bool absent = CheckAbsent(replace, Folder, stageName);
                        if (!renamed)
                        {
                            bool oldNamed = ReadNamed(replace, "failed_rename_named_head", Folder, path,
                                PublicationHead, oldId, oldContent);
                            bool stageNamed = ReadNamed(replace, "failed_rename_named_stage", Folder, path,
                                stageName, stagedId, newContent);
                            replace.Receipt["old_and_stage_preserved"] = oldAfter && oldNamed && stageNamed;
                        }
                        replace.Qualified = renamed && flushed && oldAfter && newNamed && absent;
                    }
                });
                head.Qualified = initial.Qualified && noSourceDelete.Qualified && noReaderDeleteShare.Qualified && replace.Qualified;
            }
        }

        internal Dictionary<string, object> MeasurePublication()
        {
            var writer = new PublicationPhase();
            var immutable = new PublicationPhase();
            var head = new PublicationPhase();
            writer.Measure(() =>
            {
                using (WriterHandle lockHandle = OpenLock(create: true))
                {
                    writer.Receipt["file_id"] = lockHandle.FileId;
                    writer.Receipt["volume_serial"] = lockHandle.VolumeSerial;
                    Dictionary<string, object> acquired = lockHandle.TryLock();
                    writer.Record("acquisition", acquired);
                    if (!(bool)acquired["succeeded"]) return;
                    immutable.Measure(() => MeasureImmutable(immutable));
                    head.Measure(() => MeasureHead(head));
                    VerifyFolder();
                    string finalId = NamedId(WriterName);
                    writer.Record("final_file_id", finalId);
                    writer.Qualified = lockHandle.FileId == finalId;
                }
            });
            bool qualified = writer.Qualified && immutable.Qualified && head.Qualified;
            return new Dictionary<string, object>
            {
                ["schema"] = 1,
                ["probe"] = "publication",
                ["windows_version"] = Environment.OSVersion.Version.ToString(),
                ["process_architecture"] = RuntimeInformation.ProcessArchitecture.ToString(),
                ["token"] = Token,
                ["volume"] = Volume,
                ["ordered_crash_persistence_proven"] = false,
                ["verdict"] = qualified ? "available" : "unavailable",
                ["publication_qualified"] = qualified,
                ["publication"] = new Dictionary<string, object>
                {
                    ["lock"] = writer.Receipt, ["immutable"] = immutable.Receipt, ["head"] = head.Receipt
                }
            };
        }
    }
}
