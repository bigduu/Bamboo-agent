# Deployed resident identity

Production Root `deploy_agent` persists a resident Child Session in the actual Host Store and initializes its ActorDirectory record before invoking the existing local, Docker or SSH launcher. It reserves an ActorActivation with the physical worker as an opaque placement reference before launch, then marks that activation Running after the launcher returns. The returned `actor-…` ActorId is the Session.id, with the saved caller's parent/root/birth/depth/Project. The independent broker mailbox is internal. A failed launch or failed activation start stops the worker and retires the Actor. The Host renews the activation while the deployment stays registered; failed renewal or observed process exit stops the physical worker.

The existing in-memory deployment registry associates that identity and exact activation fence with its physical handle. `ask_agent` and stop validate the caller, current Host birth/lineage, running activation, and worker placement before resolving a live ActorId or its deployment alias. `ask_agent` rechecks the binding after its broker reply. Reserved logical IDs cannot become aliases; unknown/stopped ActorIds fail before broker dispatch. Existing unbound cluster/peer routes retain their authorization behavior. A duplicate already-live alias is rejected before launch; concurrent publication never replaces the winning handle and stops the losing launch.

Stop uses the existing graceful handle shutdown, then retires the Host Actor. Failed launch attempts retain a truthful non-live identity and cancelled activation. Store reopen retains Session and Actor history. Server restart loses physical control mapping and fails closed for logical IDs; the lease expires without renewal. This slice adds no second persistent registry or restart-control recovery.

Delayed resident cleanup now retires only the exact activation it launched.
The conditional retirement runs under the existing ActorDirectory transaction,
including after its lease expires, so a replacement attempt on the same
ActorId survives a stale renewal or stop. Local and Docker deployers also
settle an already-spawned child if streaming its provision spec fails; Docker
performs its container removal in that error path. This covers a returned
launch error, not arbitrary caller cancellation during Docker startup.

This binds the local, Docker and SSH launcher paths to Host activation authority, but does not yet unify their worker-local histories with the Host transcript. Broker replies remain worker-reported, not Host transcript commits. No fenced remote transcript writer or exactly-once result publication is claimed. Focused fixtures cover Host Store activation and failed launcher rollback; broker and echo CLI coverage remains a separate integration boundary.
