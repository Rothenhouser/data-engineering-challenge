# OCPP message format (how to read the data)

Each line in the `ocpp-data-*.txt` files is one OCPP-J message: `stationId : [ ... ]`.
The data is a log of request/response traffic between chargers and the central
system. A **Call** is a request ("do this action"), answered by a **CallResult**
(success) or a **CallError** (failure). The two halves of an exchange appear as
separate lines that share one id, e.g. a `Heartbeat` request and the reply
carrying `currentTime`.

The array's first element is the **MessageTypeId** — `2` = Call, `3` = CallResult,
`4` = CallError — which also fixes the rest of the layout: a Call is
`[2, uniqueId, action, payload]`, a CallResult is `[3, uniqueId, payload]`. The
second element is the **UniqueId**, used only to match a response back to its
request; a CallResult reuses the exact UniqueId of its Call. It is unique only
per sender and connection, so it is **not** a global key — the same value recurs
across chargers and across the Call/CallResult pair.
