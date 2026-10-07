# Current ancestor checks for opt-in checkpoints

The transcript append and owned-input checkpoint ports now reuse the existing model-context checkpoint's current-ancestor observations. This repairs the old ports; it does not enable a provider, worker, Inbox release or ACK caller.

Under the existing shared lifecycle → shared Task → exact target Session guards, an owned filesystem job captures ancestor Main, Runtime, Root-tool-proof and Supervisor-proof existence and complete bytes. The existing pure ActorDirectory lineage reader independently verifies canonical parent/root/depth/birth/Project. Recorded ancestor IDs/births must match, metadata cannot roll back, and a revision gap of two or more rejects. The extracted private witness and comparison preserve the model-context port's behavior.

The final started job checks target and ancestor observations again before writing and at the real BeforeReplace barrier. Owned-input Already/recovery performs the same current check under its existing physical Inbox claim holder; a deleted middle Child or changed ancestor rejects without checkpoint publication or ACK. Transcript has no Already/replay lane. An error after replacement remains OutcomeUnconfirmed and requires a complete reload before retry.

No ancestor locks, initialization/repair, new journal, persisted format, permission grant or API are introduced. Complete started jobs retain the same actual guard Arcs through return/error cleanup and caller cancellation. Existing full-file reads and the cooperative-writer boundary remain unchanged; this does not add bounded history reads, no-follow/retained-FD security or arbitrary external-write protection.

Actual filesystem fixtures cover lawful nested success, legal deletion of a middle Child while the leaf remains, Project mismatch, coherent corrupted birth/metadata, revision gap, a legal independent parent save during BeforeReplace, all four ancestor witness files, and cold reload after unconfirmed input replacement. Existing abort/shutdown guard tests remain the ownership acceptance boundary.
