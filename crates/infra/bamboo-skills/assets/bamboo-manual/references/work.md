# Supervisor Work

Work records a deliverable, its acceptance criteria and its execution history.
Ask the Supervisor to create a draft or explicitly ask it to start a ready Work.
Dispatch starts a separate execution attempt. The owner accepts the specific
submitted result after verifying it.

## Enable Work

`features.ticket_mutation` enables Work changes and scope initialization.
`features.ticket_dispatch` enables fresh execution admission. Both flags default
to `false` and belong to the typed `providers` section. From the desktop or web
client, open **System Settings → System → Work**, enable **Allow Work changes**
and **Allow Work dispatch** as needed, then save. Service operators can inspect
the flags with `GET /api/v1/bamboo/config/sections/providers` and check the
applied `mutation_enabled` and `dispatch_enabled` values with
`GET /api/v1/tickets/scope`.
An uninitialized Supervisor scope can be set up using the Work panel's
initialization action when the service reports `can_initialize: true`.

## Review a pending message

The scope's `overview.data.pending_ingress` gives the number and oldest reference
of canonical Human messages awaiting Work resolution. Work creation and
dispatch can continue while these messages are pending. Active Work questions
and approvals appear separately. The owner can answer independent Work
questions in any order. Each answer is checked against the current request
and Work revisions.

When the Work panel offers **Review oldest message**, open it to see the original
text. If the message was ordinary chat, **Mark as chat** records a zero-operation
resolution. To resume an earlier Work instruction, tell the Supervisor which
message to use. The Supervisor can select the exact `message_id` in
`work_overview`, then call `work_update` with that ID and its proposal. A
chat-only proposal has `{"groups":[]}`. Saved proposals retain their original
identity and results.
