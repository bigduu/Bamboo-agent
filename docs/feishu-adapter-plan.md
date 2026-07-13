# Feishu adapter plan — epic #447 phase 3

Analysis re-done 2026-07-13 (worktree `feat/feishu-adapter`, zero commits yet; based on origin/main after phase 1 #453 + phase 2 #459 + config split #456/#460 + review follow-ups #462).

## 1. What already exists (reuse, do not touch)

Generic and adapter-agnostic — a Feishu adapter inherits all of it:

- `connect/mod.rs` `dispatch_loop` (mod.rs:133) routes `Inbound::Message/Callback` into the bridge.
- `bridge.rs`: allow_from exact-match on `user_id` (bridge.rs:430), stale drop (`sent_at < process_start`, :440), dedup on `"{platform}:{message_id}"` (:449), `SessionKey = platform:chat_id:user_id` (:32), busy lock + FIFO queue, `/new` `/stop` `/status`, ask fast-path.
- `render.rs`: mode picked by `capabilities().edit_message`; streaming edit-in-place throttle `EDIT_MIN_INTERVAL=1500ms` AND `EDIT_MIN_NEW_CHARS=30` (render.rs:43); `chunk_message` + `MAX_MESSAGE_CHARS=4096` exported for adapters; edit failure degrades to fresh reply (never fails the run). Output is **plain text only** (no markdown flag on `OutboundMessage`).
- `approvals.rs`: `callback_data = "{nonce}:{option_index}"` (nonce = first UUID segment); numbered text list always sent, buttons are pure enhancement; text fallback (index / exact option / 中英 yes-no intent / custom); `answer_callback` must be acked exactly once, stale ⇒ `Some("This action has expired.")`; `EngineResponder` resolution path is platform-independent.
- Test seams: adapter unit tests via `wiremock` (telegram.rs:539+ is the template incl. `with_options(token, base_url, tiny_rate_interval)` injection + token-never-leaks test); bridge/render tests use in-proc `FakePlatform`/`RecordingPlatform` — no new infra needed.

Adapter-side responsibilities (Telegram precedent): outbound rate limiting (per-chat token bucket, telegram.rs:96), message chunking in `reply`, secret redaction in every error string (`sanitize_error`, telegram.rs:177), shared `OnceLock` reqwest client.

## 2. New work items

### 2a. Config + secret plumbing (largest cross-cutting change)

`ConnectPlatformConfig` (bamboo-config config.rs:619) today has a single `token`/`token_encrypted` pair. Feishu needs **app_id (not secret) + app_secret (secret)**. Recommendation: add optional fields rather than overloading `token`:

```rust
pub app_id: Option<String>,                 // plain, serialized
pub app_secret: Option<String>,             // skip_serializing, in-memory
pub app_secret_encrypted: Option<String>,   // at-rest
pub domain: Option<String>,                 // plain, serialized — DECIDED: required config surface
```

**`domain`(已决策,必做)**:默认 `"feishu"` → `https://open.feishu.cn`;`"lark"` → `https://open.larksuite.com`;任意 `https://` 开头的值 → 私有化部署 base URL 原样使用(cc-connect 同款三态语义)。REST 与 WS bootstrap(`/callback/ws/endpoint`)共用同一 base;适配器内只存解析后的 `base_url: String`,构造时归一化(去尾部 `/`),非法值(既不是预设名也不是 https URL)在注册 arm 里 warn+skip 该 entry,与空 token 同路径。非密钥,正常序列化进 connect.json,无需进 secret 管道。

Extend all five secret round-trip sites in lockstep (the #430 contract):
1. hydrate: `hydrate_connect_platform_tokens_from_encrypted` (config_crypto.rs:509)
2. re-encrypt on save: `refresh_connect_platform_tokens_encrypted` (config_crypto.rs:538)
3. GET redaction (redaction/mod.rs:161 — positional per `platforms[i]`)
4. PATCH masked-preserve: `preserve_masked_connect_secrets` (patch.rs:422 — array order is the contract)
5. struct + connect.json round-trip tests.

Frontends must not prefill the `****...****` placeholder (existing contract).

### 2b. Registration arm

`mod.rs:68` `match platform_cfg.platform_type.as_str()` gains `"feishu" =>`: validate app_id/app_secret present, warn on empty `allow_from` (deny-all), construct `FeishuPlatform`, spawn `start()` + generic `dispatch_loop`.

### 2c. `platforms/feishu.rs` — the adapter itself

**Transport: 事件长连接 (WS), no public IP.** Protocol is SDK-only (not doc'd); verified shape:

- Bootstrap: `POST https://open.feishu.cn/callback/ws/endpoint`, body PascalCase `{"AppID","AppSecret","ClientAssertion":""}` → `data.URL` (wss, single-use — re-fetch on every reconnect) + `ClientConfig{PingInterval(默认2min), ReconnectInterval(2min fixed), ReconnectNonce(30s jitter), ReconnectCount(-1)}`. Parse `service_id` from the wss URL query — echoed in ping frames. Fatal (stop reconnecting): non-{0,1,1000040343} bootstrap code, handshake `Handshake-Status:403`, `Handshake-Autherrcode:1000040350` (>50 conns/app).
- Wire: binary proto2 `pbbp2.Frame{SeqID,LogID,service,method(0=ctrl/1=data),headers[{key,value}],payload,...}`. Ping = method 0 + header `type:ping` every PingInterval; pong may carry retuned ClientConfig JSON. Data frames: header `type:event`, payload = plaintext schema-2.0 event JSON (no encrypt-key on WS path). Multi-part: `sum`/`seq` headers keyed by `message_id`, 5s TTL, ack only when complete. **Ack = echo the same frame back with payload `{"code":200,"headers":null,"data":<base64>}` within 3s** or the server re-pushes; for `card.action.trigger` the base64 `data` carries the callback response JSON (toast/card).
- Dependency: vendor `lark-websocket-protobuf` (MIT/Apache, prost Frame/Header only) + hand-rolled client on `reqwest + tokio-tungstenite + prost` (~1k lines; openlark-client is the reference implementation but pulls an 18-crate workspace and has no built-in reconnect anyway). **Check tokio-tungstenite TLS feature against the native-tls pin (no aws-lc-sys)** — `feishu-sdk` crate is rejected precisely for pinning rustls+aws-lc-rs. Prior art: github.com/linuxhenhao/beam (Rust, same stack, same use case).
- REST: `tenant_access_token/internal` (7200s; Feishu returns the SAME token while ≥30min remain — refresh when <30min or on 99991663/99991661), send `POST /open-apis/im/v1/messages?receive_id_type=chat_id` (`content` is a JSON-escaped string; text 150KB / card 30KB; optional `uuid` send-dedup), card update `PATCH /open-apis/im/v1/messages/:message_id`.

**Inbound mapping** (`im.message.receive_v1`, scopes `im:message.*`):
- `chat_id` = `message.chat_id`; `user_id` = `sender.sender_id.open_id` (**allow_from entries are open_id** — document); `message_id` = `message.message_id` (official dedup guidance; NOT event_id for messages); `sent_at` = `message.create_time` (ms string); `text` = parse `content` JSON `{"text":...}`, strip `@_user_N` mention placeholders.
- `reply_ctx` = `{"chat_id":..., "message_id":...}` (message_id enables threaded reply later).
- Group gating (adapter-side; bridge has no such concept): MVP = p2p + @mention-required in groups (bot open_id in `mentions`), cc-connect precedent. Drop `sender_type:"bot"`.
- Card callback (`card.action.trigger`): `user_id` = `operator.open_id`, `chat_id` = `context.open_chat_id`, `data` = `action.value` round-tripped verbatim (put bamboo's `"{nonce}:{index}"` inside `value`), dedup on `event_id` (create_time is **microseconds** here vs ms on messages).

**Capabilities**: `{buttons:true, edit_message:true, images:false, files:false}`.

**cc-connect 飞书源码验证过的坑** (github.com/chenhg5/cc-connect `platform/feishu/`, 2026-07-13 main):
- **同 app 只能开一条 WS**: Feishu 对同一 app 的多条长连接做随机负载均衡(每个事件只发给一条连接),cc-connect 为此实现了 sharedWSGroup(首个实例持有连接、事件扇出给同 app 兄弟)。bamboo 一个 config entry = 一个连接,天然安全;但绝不能在重连时短暂并存两条连接处理逻辑。
- Bot 自身 open_id 需启动时取一次 `GET /open-apis/bot/v3/info`(@mention 判定用);失败则降级为不做群过滤 + warn。
- `@所有人` 消息 mentions 数组为空,文本含 `@_all` —— 单独的 substring 判断。
- 文本里的 mention 是 `@_user_N` 占位符,按 `mentions[].key` 替换:bot 自己的删掉、他人替换为 `@显示名`。
- msg_type 决策树:含 `<at>` 标签或纯文本 → `text`(mention 事件只在 text 消息触发);markdown 表格 >5 个 → `post`(card 超 5 表报 11310);其余 → `interactive` card。
- 回调响应最佳实践:同步返回**替换后的卡片**(按钮消失 = 天然防双击),决定后的卡片内容(label/颜色/正文)直接塞在按钮 `value` 里,回调无需查状态。
- 999916 63 invalid-token → 禁缓存强刷 token 重试一次;所有发送包 3 次指数退避 transient 重试(仅网络类错误)。
- 流式 PUT 撞 230020 限流时直接丢帧(下一次 flush 会带全量文本),不重试。

**Outbound mapping — key design decisions**:
1. **The streaming status message must be an interactive card from the first send**, never text: text `PUT` edit has a 20-edit cap (230072) — unusable for streaming; card `PATCH` has no count cap, 5 QPS/message, 14-day window. render.rs throttle (≥1.5s between edits) stays well under 5 QPS. `MessageRef = {"message_id","msg_type"}`; `edit` on a text ref returns Err → render degrades gracefully.
2. Card shape: schema 2.0, `config.update_multi:true`, markdown element with fixed `element_id:"main_text"`, buttons as `{"tag":"button","behaviors":[{"type":"callback","value":{"cb":"{nonce}:{idx}"}}]}`. Plain-text-in-markdown: escape or use plain_text element to avoid render.rs output being reinterpreted as markdown.
3. Plain replies without buttons: `msg_type:"text"` and chunk with the shared `chunk_message` (Feishu text limit 150KB ≫ 4096, so the shared cap is safe).
4. **answer_callback ≠ a REST call on Feishu — it IS the WS frame ack.** Adapter keeps a pending-ack map `callback_query_id → oneshot<frame-ack>`; `start()`'s card.action.trigger handler parks the frame, bridge calls `answer_callback(id, text?)` → resolve with `{"toast":{...}}` (and optionally a "decided" card); auto-ack `{"code":200}` after ~2.5s if the bridge hasn't responded (3s hard deadline). This is the one genuine impedance mismatch with the `Platform` trait — solve it inside the adapter, don't change the trait.
5. Rate limiter: per-chat token bucket like Telegram (limits: 5 QPS/user p2p, 5 QPS/group shared by ALL bots, app 1000/min); honor `x-ogw-ratelimit-reset` on 99991400/429 and 230020; blocking, never dropping.
6. Secrets: app_secret and tenant_access_token must never appear in errors/logs (Telegram `sanitize_error` + leak test pattern); token lives in the Authorization header (not URL, easier than Telegram).

### 2d. Deferred (post-MVP, in-epic)
- cardkit typewriter streaming (`POST /cardkit/v1/cards` streaming_mode + sequence PUTs, 10 calls/s/card; NOTE: once a message references a card entity, im/v1 PATCH silently no-ops — entity cards update only via cardkit).
- Threaded reply / thread_isolation, group shared-session, reaction acks (👀/✅), images/files, Lark intl domain option (`open.larksuite.com`), webhook fallback mode.
- Multi-bot `SessionKey` disambiguation: nothing today distinguishes two entries of the same platform type (shared dedup set + colliding SessionKey). Single feishu entry is fine; if multi-app needed, fold app_id into the `platform` string (`"feishu:cli_xxx"`) — cheaper than changing SessionKey.

## 3. Suggested implementation order
1. Config fields + 5-site secret plumbing + connect.json tests.
2. Vendored pbbp2 proto + WS client (bootstrap/ping/ack/reassembly/reconnect) behind a trait-less internal module, unit-tested with a local WS stub.
3. `FeishuPlatform` REST half (token cache, reply/edit/rate limiter) with wiremock tests mirroring telegram.rs.
4. Registration arm + inbound mapping + pending-ack bridge for callbacks.
5. Live spike with real app credentials (真机 e2e, like tg-e2e worktree did for Telegram) before PR.

## 4. Open questions for the maintainer
- allow_from ID type: open_id (app-scoped, recommended) vs user_id (tenant-scoped, needs extra scope)?
- MVP group policy: p2p-only vs @mention-gated groups?

Decided 2026-07-13: `domain` config field ships in the MVP (feishu/lark/自定义 https base URL,见 §2a) — maintainer confirmed Lark support is required.
