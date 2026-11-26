# P08 · Decentralized Secure Messaging

**English** · [Español](README.es.md)

A messaging service where users can be offline, where no central database keeps
the conversations, and where only the two people in a conversation can read it.

Final project for *Distributed Systems* (2026), Faculty of Mathematics and
Computer Science, University of Havana.

## The problem

When users may be disconnected and no central database is allowed to store
every conversation permanently, messaging requires deferred delivery, discovery,
ordering, retries, duplicate suppression, local persistence, identity and
privacy.

**Goal:** build a decentralized messaging service with user identity, contact
discovery, one-to-one conversations, delivery to offline recipients, and
authenticated, encrypted communication.

**Scope required by the project statement**

| Requirement | How this project meets it |
|---|---|
| User identity and contact discovery | A user **is** a key pair: the id is the hash of the public key, so nobody assigns or approves it. Contacts are added by sharing a **signed contact link**. A replicated directory tells who is online right now |
| One-to-one conversations | Each pair of users shares one conversation |
| Delivery to offline recipients | Messages wait in **mailboxes** replicated on 3 service nodes |
| Deferred delivery, store-and-forward | Each client keeps a durable outbox and retries with exponential backoff and jitter until a mailbox quorum (2 of 3 nodes) accepts the message |
| Message ordering | Per-sender FIFO order with gap detection, plus **Lamport timestamps** so both participants see the same history, in causal order, whatever their wall clocks say |
| Duplicate suppression | Every message has a unique id; the receiver remembers which ids it has stored and nodes store each envelope once |
| Local persistence of the history | SQLite on each device: history, outbox, contacts and ordering state survive a restart |
| Authentication and encryption | End-to-end: encrypt-then-sign with libsodium (PyNaCl). Nodes only see who sends to whom, never the text, and cannot forge or alter a message |
| Temporary relays, no permanent central database | Mailboxes only keep a message until its receiver collects it, or until it expires |

## Status: Phase 1, prototype on Ray

The course has three phases. **This repository is in Phase 1**: the whole
system runs distributed over [Ray](https://www.ray.io/) actors, with each
service node and each user's device in its own process. In Phase 2 Ray is
replaced by our own networking. The architecture keeps Ray inside a few
adapters, so the rest of the code does not change.

**Working now**

- Accounts protected by a password (Argon2id), contacts by signed link, contact requests.
- Direct delivery between online users, falling back to the mailboxes.
- Mailboxes replicated on 3 nodes with a 2-of-3 quorum. Any single node can fail without losing messages.
- Collection on reconnection, acknowledgement, duplicate suppression.
- FIFO ordering with gaps ("message not available yet") and Lamport-ordered history.
- A launcher that starts the cluster from a config file, and fault tools to kill and restart nodes.
- A text interface for each user, and structured JSON logs for every process.

**Not done yet / known limits**

- **Delivery receipts through the mailbox.** A message delivered directly shows ✓✓, but one that went through a mailbox stays at ✓ even after it is read.
- **Purging expired data on the nodes.** Expired messages are never delivered, but they are not yet deleted from the nodes' disks.

## How it works

```
   device "alice"                        device "bob"
 ┌───────────────────┐                ┌───────────────────┐
 │ ClientApp + SQLite│── direct ─────▶│ ClientApp + SQLite│   both online: straight to Bob
 └────────┬──────────┘                └─────────▲─────────┘
          │ deposit (2 of 3)                    │ collect when he connects
          ▼                                     │
 ┌──────────┐  ┌──────────┐  ┌──────────┐       │
 │  node-1  │  │  node-2  │  │  node-3  │ ──────┘   mailboxes + presence directory
 └──────────┘  └──────────┘  └──────────┘           (they only see encrypted envelopes)
```

- **Clients** (one per device) hold the keys, the history and the outbox. Every
  message is encrypted for its receiver and signed by its sender before it
  leaves the device.
- **Service nodes** keep two things, replicated with quorums: the **mailboxes**
  (encrypted envelopes waiting for offline users) and the **presence
  directory** (signed "I am online until…" records).
- When the receiver is online the message goes **directly**. Otherwise it is
  deposited in the receiver's mailboxes and collected when they connect.

The code follows a **hexagonal architecture**: the domain and the use cases
know nothing about Ray, SQLite or PyNaCl. These are adapters behind ports, and
an automatic check (import-linter) enforces it.

```
source/
  domain/         rules: envelope format, ordering, Lamport, contacts, quorum math
  application/    use cases (identity, send, receive, contacts, delivery, mailbox,
                  directory, quorum) and the ports they need
  adapters/       memory (tests), system (SQLite, PyNaCl, clock, Ray actors), cli
  composition/    wiring: client builder, Ray network, launcher, faults, CLI entry
tests/            unit, contract, integration (Ray) and end-to-end tests
tools/logview.py  reads and merges the JSON logs of every process
config/           cluster.json: the default configuration of the system
docs/             technical documentation 
```

## Requirements

- **Python 3.10, 3.11 or 3.12**
- **Linux or macOS** (Ray on Windows works best through WSL)
- About 2 GB of free RAM: Ray starts several processes

## Installation

```bash
git clone https://github.com/Bazan49/Distributed-Systems-Project.git
cd Distributed-Systems-Project

python3 -m venv .venv
source .venv/bin/activate          # every new terminal needs this line

pip install -e ".[dev]"            # the app (Ray, PyNaCl) and the dev tools
```

## Running the system, step by step

Use one terminal for the service nodes and one more per user, all in the
project folder and with `source .venv/bin/activate` run first.

### 1. Start the service nodes (terminal 1)

```bash
python -m source.composition.launcher
```

Wait for `READY`. The launcher starts Ray on this machine and the 3 service
nodes described in [config/cluster.json](config/cluster.json), the default
configuration.
Leave it running: it is the infrastructure (mailboxes and directory).

### 2. Open one interface per user (terminals 2, 3…)

```bash
python -m source.composition.cli --device alice
```

```bash
python -m source.composition.cli --device bob
```

Any name works (lowercase letters, digits, `-` and `_`), and there can be as
many users as terminals: nobody has to be declared beforehand. Each command
joins the running cluster and turns on that **device**, whose account and
history are stored in `data/cluster/devices/<name>.db`. Closing the interface turns the
device off: the user goes offline, and their messages wait in the mailboxes.
Opening it again with the same name logs back into the same account.

### 3. (Optional) Follow the logs live (another terminal)

```bash
tail -f logs/*.jsonl | python -m tools.logview
```

### 4. Stop everything

Close the interfaces (`0. Salir`), then press `Ctrl+C` in the launcher: it
stops the nodes and Ray. Accounts, contacts, histories and pending messages
stay in `data/`. Launch again and everything is as you left it.

### The configuration file

```json
{
  "namespace": "mensajeria",
  "data_dir": "../data/cluster",
  "nodes": ["node-1", "node-2", "node-3"],
  "seeds": ["node-1", "node-2"],
  "clients": []
}
```

| Key | Meaning |
|---|---|
| `nodes` | Service nodes to start. Three is the minimum for the 2-of-3 quorum: any one of them can fail |
| `seeds` | The nodes clients are told about. A client asks any live seed for the full list of nodes |
| `namespace` | Ray namespace where every actor is looked up by name. Launcher and interfaces must use the same one |
| `data_dir` | Folder of the nodes' databases, relative to the configuration file (here `data/cluster/`) |
| `clients` | Devices to start **together with the nodes**, for scripted experiments (e.g. `[{"id": "alice"}]`). They keep running when their interface closes, like a phone with its screen off, and keep their data in the same place as any other device, so a name is always the same account For normal use leave it empty and start devices from the interface |
| `ray_address` | Optional. Without it the launcher uses the Ray cluster already running on this machine, or starts one. Give an address to join a cluster on another machine |

The interface reads **the same file**, so both always agree on the namespace,
the nodes and where each device keeps its data. To try other settings, copy it
to `config/cluster.local.json` (ignored by git) and give it to both:
`python -m source.composition.launcher config/cluster.local.json` and
`python -m source.composition.cli --device alice --config config/cluster.local.json`.

## Tests and quality checks

```bash
pytest                       # whole suite: about 1,700 tests, Ray ones included (~2.5 min)
pytest -m "not integration"  # without Ray: seconds
ruff check . && ruff format --check .   # lint and format
mypy .                       # strict typing
lint-imports                 # architecture rules (layers, no I/O in the domain)
```

The same checks run on every push in GitHub Actions, on Python 3.10, 3.11 and
3.12. Coverage must stay at 90 % or more.

```bash
pre-commit install           # once: runs the checks before every commit
```

## Logs

Every process writes structured events, one JSON object per line, to
`logs/<process>.jsonl`: `node-1.jsonl`, `client-alice.jsonl`… Message texts,
passwords, keys and aliases are never written. To follow one message through
every process:

```bash
python -m tools.logview logs/*.jsonl --msg <message id or a prefix>
```
