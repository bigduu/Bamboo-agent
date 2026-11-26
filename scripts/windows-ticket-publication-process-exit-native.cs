public static partial class WindowsTicketDirectoryFlushProbe
{
    private const string ProcessHeadStage = "head-process-exit-stage";
    private const int ProcessAddressLength = 64;
    private const int ProcessRecordLimit = 256;
    public const int PublicationProcessCutExitCode = 0x4254;

    [DllImport("kernel32.dll", ExactSpelling = true, SetLastError = true)]
    private static extern bool TerminateProcess(SafeProcessHandle process, uint exitCode);

    public static int TerminatePublicationProcess(SafeProcessHandle writer) =>
        TerminateProcess(writer, PublicationProcessCutExitCode) ? 0 : FailureCode("Terminate the publication writer");

    public static void WaitAtPublicationProcessCut()
    {
        // Console.In may perform even its async read synchronously.
        var input = System.Threading.Tasks.Task.Run(() => Console.ReadLine());
        if (!input.Wait(TimeSpan.FromSeconds(45)))
            throw new TimeoutException("The publication writer cut timed out.");
        throw new OperationCanceledException("The publication writer cut was interrupted without process termination.");
    }

    public static Dictionary<string, object> RunPublicationProcessWriter(string expectedSid, string fixtureDirectory,
        string cut, Action<Dictionary<string, object>> reportCut)
    {
        if (cut != "complete" && cut != "before-head-replace" && cut != "after-head-replace" && cut != "after-scope-flush")
            throw new ArgumentException("Unknown publication process exit boundary.", nameof(cut));
        if (cut != "complete" && reportCut == null) throw new ArgumentNullException(nameof(reportCut));
        using (WriterScope scope = OpenWriterScope(expectedSid, fixtureDirectory, requireEmpty: true))
            return scope.MeasureProcessWriter(cut, reportCut);
    }

    public static Dictionary<string, object> RunPublicationProcessVerifier(string expectedSid, string fixtureDirectory)
    {
        using (WriterScope scope = OpenWriterScope(expectedSid, fixtureDirectory, requireEmpty: false))
            return scope.MeasureProcessVerifier();
    }

    public sealed partial class WriterScope
    {
        // A bounded scratch format with the same named HEAD and immutable hash links as the ledger.
        private sealed class ProcessGraph
        {
            public int Sequence { get; }
            public string Parent { get; }
            public byte[] ObjectBytes { get; }
            public byte[] ManifestBytes { get; }
            public byte[] CommitBytes { get; }
            public string ObjectHash { get; }
            public string ManifestHash { get; }
            public string CommitHash { get; }

            public ProcessGraph(int sequence, string parent)
            {
                Sequence = sequence;
                Parent = parent;
                ObjectBytes = Encoding.ASCII.GetBytes($"ticket-object-v1\nsequence={sequence}\n");
                ObjectHash = ContentName(ObjectBytes);
                ManifestBytes = Encoding.ASCII.GetBytes($"ticket-manifest-v1\nobject={ObjectHash}\n");
                ManifestHash = ContentName(ManifestBytes);
                CommitBytes = Encoding.ASCII.GetBytes($"ticket-commit-v1\nsequence={sequence}\nparent={parent ?? "-"}\nmanifest={ManifestHash}\n");
                CommitHash = ContentName(CommitBytes);
            }

            public Dictionary<string, object> Describe() => new Dictionary<string, object>
            {
                ["sequence"] = Sequence, ["parent"] = Parent, ["commit_hash"] = CommitHash,
                ["manifest_hash"] = ManifestHash, ["object_hash"] = ObjectHash
            };
        }

        private sealed class ProcessDirectory : IDisposable
        {
            public SafeFileHandle Handle { get; }
            public string Path { get; }
            public string Id { get; }

            public ProcessDirectory(SafeFileHandle handle, string path, string id)
            {
                Handle = handle;
                Path = path;
                Id = id;
            }

            public void Dispose() => Handle.Dispose();
        }

        private ProcessDirectory OpenProcessDirectory(PublicationPhase phase, string name, bool create)
        {
            uint access = WriterDirectoryAccess | (create ? GenericWrite : 0);
            SafeFileHandle handle = OpenPublicationObject(phase, name + "_directory", Folder, name,
                access, WriterShare, create ? NtCreate : NtOpen, directory: true);
            try
            {
                string fullName = Path.Combine(path, name);
                FileIdentity identity = PublicationIdentity(handle, fullName, true);
                return new ProcessDirectory(handle, fullName, identity.Id);
            }
            catch { handle.Dispose(); throw; }
        }

        private Dictionary<string, object> ProcessReceipt(string role, PublicationPhase trace,
            Dictionary<string, object> lockReceipt) => new Dictionary<string, object>
        {
            ["schema"] = 1,
            ["probe"] = "publication-process-exit",
            ["role"] = role,
            ["pid"] = Environment.ProcessId,
            ["windows_version"] = Environment.OSVersion.Version.ToString(),
            ["process_architecture"] = RuntimeInformation.ProcessArchitecture.ToString(),
            ["token"] = Token,
            ["volume"] = Volume,
            ["lock"] = lockReceipt,
            ["operations"] = trace.Receipt,
            ["ordered_crash_persistence_proven"] = false
        };

        private static Dictionary<string, object> FinishProcessReceipt(Dictionary<string, object> receipt,
            bool qualified, string successRecord)
        {
            receipt["qualified"] = qualified;
            receipt["verdict"] = qualified ? "available" : "unavailable";
            receipt["record"] = qualified ? successRecord : "unavailable";
            return receipt;
        }

        private static bool AcquireProcessLock(PublicationPhase trace, WriterHandle handle,
            Dictionary<string, object> lockReceipt)
        {
            lockReceipt["file_id"] = handle.FileId;
            lockReceipt["volume_serial"] = handle.VolumeSerial;
            var operation = handle.TryLock();
            lockReceipt["acquisition"] = operation;
            trace.Record("lock_acquisition", operation);
            return (bool)operation["succeeded"];
        }

        private static void RequireProcessEvidence(bool observed, string message)
        {
            if (!observed) throw new IOException(message);
        }

        private void InstallProcessNode(PublicationPhase phase, ProcessDirectory directory,
            string stageName, string address, byte[] bytes)
        {
            InstallNewName(phase, directory.Handle, directory.Path, directory.Id, stageName, address, bytes);
            RequireProcessEvidence(phase.Qualified, "The process fixture did not publish a qualified immutable node.");
        }

        private void InstallProcessGraph(PublicationPhase phase, ProcessDirectory objects, ProcessDirectory manifests,
            ProcessDirectory commits, ProcessGraph graph, string prefix)
        {
            InstallProcessNode(phase.Child("object"), objects, prefix + "-object-stage", graph.ObjectHash, graph.ObjectBytes);
            InstallProcessNode(phase.Child("manifest"), manifests, prefix + "-manifest-stage", graph.ManifestHash, graph.ManifestBytes);
            InstallProcessNode(phase.Child("commit"), commits, prefix + "-commit-stage", graph.CommitHash, graph.CommitBytes);
            phase.Qualified = true;
        }

        private Dictionary<string, object> DescribeProcessHead(string id, string commit, byte[] bytes) =>
            new Dictionary<string, object>
            {
                ["file_id"] = id, ["volume_serial"] = volumeSerial, ["commit_hash"] = commit,
                ["content_hash"] = ContentName(bytes), ["bytes_hex"] = Hex(bytes)
            };

        private static void ReportProcessCut(string selected, string reached, Dictionary<string, object> receipt,
            Action<Dictionary<string, object>> reportCut)
        {
            if (selected != reached) return;
            reportCut(FinishProcessReceipt(receipt, true, "cut_reached"));
            throw new OperationCanceledException("The cut callback returned while the writer should be held for process exit.");
        }

        internal Dictionary<string, object> MeasureProcessWriter(string cut, Action<Dictionary<string, object>> reportCut)
        {
            var trace = new PublicationPhase();
            var lockReceipt = new Dictionary<string, object> { ["acquisition"] = Operation(null) };
            var receipt = ProcessReceipt("writer", trace, lockReceipt);
            receipt["cut"] = cut;
            trace.Measure(() =>
            {
                using (WriterHandle locked = OpenLock(create: true))
                {
                    if (!AcquireProcessLock(trace, locked, lockReceipt)) return;
                    using (var writable = OpenPublicationScope(trace))
                    using (ProcessDirectory objects = OpenProcessDirectory(trace, "objects", create: true))
                    using (ProcessDirectory manifests = OpenProcessDirectory(trace, "manifests", create: true))
                    using (ProcessDirectory commits = OpenProcessDirectory(trace, "commits", create: true))
                    {
                        var old = new ProcessGraph(1, null);
                        var next = new ProcessGraph(2, old.CommitHash);
                        InstallProcessGraph(trace.Child("old_graph"), objects, manifests, commits, old, "old");
                        receipt["old_graph"] = old.Describe();
                        byte[] oldBytes = Encoding.ASCII.GetBytes(old.CommitHash);
                        var initial = trace.Child("initial_head");
                        string oldId = InstallNewName(initial, writable, path, folderId, "head-seed-stage", PublicationHead, oldBytes);
                        RequireProcessEvidence(initial.Qualified, "The process fixture did not seed the old HEAD.");
                        receipt["old_head"] = DescribeProcessHead(oldId, old.CommitHash, oldBytes);

                        InstallProcessGraph(trace.Child("new_graph"), objects, manifests, commits, next, "new");
                        receipt["new_graph"] = next.Describe();
                        byte[] newBytes = Encoding.ASCII.GetBytes(next.CommitHash);
                        var head = trace.Child("head");
                        using (var stage = OpenPublicationObject(head, "stage_create", Folder, ProcessHeadStage,
                            GenericRead | GenericWrite | WriterDelete | WriterSynchronize, PublicationShare, NtCreate, directory: false))
                        {
                            string stageId = WriteStage(head, stage, Path.Combine(path, ProcessHeadStage), newBytes);
                            RequireProcessEvidence(stageId != oldId, "The next staged HEAD did not obtain a distinct identity.");
                            receipt["staged_head"] = DescribeProcessHead(stageId, next.CommitHash, newBytes);
                            ReportProcessCut(cut, "before-head-replace", receipt, reportCut);
                            var rename = PublicationRename(head, stage, Folder, PublicationHead, RenameReplace | RenamePosix);
                            RequireProcessEvidence((bool)rename["succeeded"], "The fixture HEAD replacement was not confirmed.");
                            ReportProcessCut(cut, "after-head-replace", receipt, reportCut);
                            bool flushed = FlushPublicationDirectory(head, "directory_flush", writable, path, folderId);
                            RequireProcessEvidence(flushed, "The fixture HEAD directory flush was not confirmed.");
                            ReportProcessCut(cut, "after-scope-flush", receipt, reportCut);

                            bool named = ReadNamed(head, "named_head", Folder, path, PublicationHead, stageId, newBytes);
                            bool absent = CheckAbsent(head, Folder, ProcessHeadStage);
                            head.Qualified = named && absent;
                            VerifyFolder();
                            string finalLockId = NamedId(WriterName);
                            trace.Record("final_lock_file_id", finalLockId);
                            lockReceipt["final_file_id"] = finalLockId;
                            trace.Qualified = head.Qualified && finalLockId == locked.FileId;
                        }
                    }
                }
            });
            return FinishProcessReceipt(receipt, trace.Qualified, "complete");
        }

        private static string ProcessText(byte[] bytes)
        {
            if (bytes == null) throw new IOException("The process fixture record could not be read.");
            foreach (byte unit in bytes)
                if (unit > 0x7f) throw new IOException("The process fixture record is not ASCII.");
            return Encoding.ASCII.GetString(bytes);
        }

        private static string ProcessAddress(string address)
        {
            if (address.Length != ProcessAddressLength)
                throw new IOException("A process fixture address has the wrong length.");
            foreach (char unit in address)
                if (!(unit >= '0' && unit <= '9') && !(unit >= 'a' && unit <= 'f'))
                    throw new IOException("A process fixture address is not lowercase hexadecimal.");
            return address;
        }

        private static string ProcessField(string line, string label)
        {
            if (!line.StartsWith(label, StringComparison.Ordinal))
                throw new IOException("A process fixture record has an invalid field.");
            return line.Substring(label.Length);
        }

        private bool ProcessFileIdentity(Dictionary<string, object> observation) =>
            observation.TryGetValue("file_id", out object id) && id is string fileId && fileId.Length == 32 &&
            (string)observation["volume_serial"] == volumeSerial;

        private byte[] ReadProcessNode(PublicationPhase phase, string operation, ProcessDirectory directory, string address)
        {
            byte[] bytes = ReadPublicationName(phase, operation, directory.Handle, directory.Path, address, ProcessRecordLimit,
                out Dictionary<string, object> observed, out _);
            RequireProcessEvidence(bytes != null && bytes.Length <= ProcessRecordLimit && ProcessFileIdentity(observed),
                "A process fixture graph node is unreadable or has the wrong identity.");
            RequireProcessEvidence(ContentName(bytes) == address, "A process fixture graph node has an invalid content address.");
            return bytes;
        }

        private ProcessGraph ReadProcessGraph(PublicationPhase phase, ProcessDirectory objects, ProcessDirectory manifests,
            ProcessDirectory commits, string commitHash)
        {
            byte[] commit = ReadProcessNode(phase, "commit", commits, commitHash);
            string[] fields = ProcessText(commit).Split('\n');
            RequireProcessEvidence(fields.Length == 5 && fields[0] == "ticket-commit-v1" && fields[4] == "",
                "The process fixture commit is malformed.");
            string sequence = ProcessField(fields[1], "sequence=");
            string parent = ProcessField(fields[2], "parent=");
            string manifestHash = ProcessAddress(ProcessField(fields[3], "manifest="));
            var expectedOld = new ProcessGraph(1, null);
            ProcessGraph expected;
            if (sequence == "1" && parent == "-") expected = expectedOld;
            else if (sequence == "2" && ProcessAddress(parent) == expectedOld.CommitHash)
                expected = new ProcessGraph(2, parent);
            else throw new IOException("The process fixture commit sequence and parent do not match.");

            // Select both descendants from the opened records, never from a writer receipt or a guessed name.
            byte[] manifest = ReadProcessNode(phase, "manifest", manifests, manifestHash);
            string[] entries = ProcessText(manifest).Split('\n');
            RequireProcessEvidence(entries.Length == 3 && entries[0] == "ticket-manifest-v1" && entries[2] == "",
                "The process fixture manifest is malformed.");
            string objectHash = ProcessAddress(ProcessField(entries[1], "object="));
            byte[] data = ReadProcessNode(phase, "object", objects, objectHash);
            RequireProcessEvidence(commit.AsSpan().SequenceEqual(expected.CommitBytes) &&
                manifest.AsSpan().SequenceEqual(expected.ManifestBytes) && data.AsSpan().SequenceEqual(expected.ObjectBytes),
                "The process fixture graph does not match the exact scratch content.");
            phase.Qualified = true;
            return expected;
        }

        private string DescribeReadHead(byte[] bytes, Dictionary<string, object> observed, out Dictionary<string, object> head)
        {
            RequireProcessEvidence(bytes != null && ProcessFileIdentity(observed), "The process fixture HEAD is unreadable.");
            string address = ProcessAddress(ProcessText(bytes));
            head = DescribeProcessHead((string)observed["file_id"], address, bytes);
            return address;
        }

        internal Dictionary<string, object> MeasureProcessVerifier()
        {
            var trace = new PublicationPhase();
            var lockReceipt = new Dictionary<string, object> { ["acquisition"] = Operation(null) };
            var receipt = ProcessReceipt("verifier", trace, lockReceipt);
            receipt["staged_head"] = null;
            receipt["stage_absent"] = false;
            trace.Measure(() =>
            {
                using (WriterHandle locked = OpenLock(create: false))
                {
                    if (!AcquireProcessLock(trace, locked, lockReceipt)) return;
                    byte[] named = ReadPublicationName(trace, "named_head", Folder, path, PublicationHead, ProcessAddressLength,
                        out Dictionary<string, object> current, out _);
                    string address = DescribeReadHead(named, current, out Dictionary<string, object> head);
                    receipt["named_head"] = head;
                    using (ProcessDirectory objects = OpenProcessDirectory(trace, "objects", create: false))
                    using (ProcessDirectory manifests = OpenProcessDirectory(trace, "manifests", create: false))
                    using (ProcessDirectory commits = OpenProcessDirectory(trace, "commits", create: false))
                    {
                        ProcessGraph graph = ReadProcessGraph(trace.Child("named_graph"), objects, manifests, commits, address);
                        receipt["graph"] = graph.Describe();
                        byte[] staged = ReadPublicationName(trace, "staged_head", Folder, path, ProcessHeadStage, ProcessAddressLength,
                            out Dictionary<string, object> stage, out int stageStatus);
                        if (stageStatus == ObjectNameNotFound || stageStatus == NoSuchFile)
                        {
                            receipt["stage_absent"] = true;
                            RequireProcessEvidence(graph.Sequence == 2, "The previous HEAD is still named and the staged name is absent.");
                            ProcessGraph previous = ReadProcessGraph(trace.Child("previous_graph"), objects, manifests, commits, graph.Parent);
                            RequireProcessEvidence(previous.Sequence == 1, "The named new graph does not have the complete old parent.");
                            receipt["previous_graph"] = previous.Describe();
                        }
                        else
                        {
                            string stagedAddress = DescribeReadHead(staged, stage, out Dictionary<string, object> stagedHead);
                            receipt["staged_head"] = stagedHead;
                            ProcessGraph next = ReadProcessGraph(trace.Child("staged_graph"), objects, manifests, commits, stagedAddress);
                            RequireProcessEvidence(graph.Sequence == 1 && next.Sequence == 2 && next.Parent == address &&
                                (string)head["file_id"] != (string)stagedHead["file_id"],
                                "The staged graph is not a complete, distinct successor to named HEAD.");
                            receipt["staged_graph"] = next.Describe();
                        }
                    }
                    VerifyFolder();
                    string finalLockId = NamedId(WriterName);
                    trace.Record("final_lock_file_id", finalLockId);
                    lockReceipt["final_file_id"] = finalLockId;
                    trace.Qualified = finalLockId == locked.FileId;
                }
            });
            return FinishProcessReceipt(receipt, trace.Qualified, "verification");
        }
    }
}
