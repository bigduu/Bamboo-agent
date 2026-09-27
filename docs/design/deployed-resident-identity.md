# Deployed resident identity

Production Root `deploy_agent` persists a resident Child Session in the actual Host Store and initializes its Cold ActorDirectory record before invoking the existing local, Docker or SSH launcher. The returned `actor-…` ActorId is that Session.id, with the saved caller's parent/root/birth/depth/Project. The independent broker mailbox is internal. Process deployment does not claim an ActorActivation or prove worker output.

The existing in-memory deployment registry associates that identity with its physical handle. `ask_agent` and stop validate the caller and the current Host birth/lineage before resolving a live ActorId or its deployment alias. Reserved logical IDs cannot become aliases; unknown/stopped ActorIds fail before broker dispatch. Existing unbound cluster/peer routes retain their authorization behavior. A duplicate already-live alias is rejected before launch; concurrent publication never replaces the winning handle and stops the losing launch.

Stop uses the existing graceful handle shutdown, then retires the Host Actor. Failed launch attempts retain a truthful non-live identity. Cold Store reopen retains Session and Actor history. Server restart loses physical control mapping and fails closed for logical IDs; this slice adds no second persistent registry or restart-control recovery.

This is identity mapping only. Legacy workers retain their own local histories; broker replies are worker-reported, not Host transcript commits. No remote activation, lease/expiry, fenced remote transcript writer, ExactlyOnce, or deployment credential exposure is claimed. Focused fixtures exercise an actual broker and echo CLI via the normal tools plus real Host Store reopen and invalid caller/birth rejection.
