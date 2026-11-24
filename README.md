# Module 8 Assignment: Replication & Consistency

This project simulates a small inventory system with one Primary replica and one Secondary replica. Writes happen on the Primary first, and an asynchronous worker propagates each versioned update to the Secondary.

## Architecture

```text
                 write(stock)
Client ------------------------------> Primary
  |                                      |
  | read                                 | asynchronous update queue
  |                                      v
  +-------------------------------> Secondary
               read                 (replica)
```

The replication worker can be paused and resumed. While it is paused, the Primary can contain a newer value than the Secondary, creating a stale-read window.

## Requirements

- Python 3.10 or newer
- No third-party packages

## Run the program

```bash
python3 inventory_replication.py
```

## Evidence

The following output was produced by running the program:

```text
=== Module 8: Replication & Consistency ===
READ     initial state          Primary=10 (v0) | Secondary=10 (v0)

--- 1. Normal replication ---
WRITE  Primary   stock=8 version=1
REPLICATE Secondary stock=8 version=1
READ     after replication      Primary=8 (v1) | Secondary=8 (v1)
RESULT   Normal replication successful: both replicas return 8

--- 2. Stale read test ---
CONTROL Replication paused
WRITE  Primary   stock=9 version=2
READ     during replication pause Primary=9 (v2) | Secondary=8 (v1)
RESULT   Stale read observed: Primary=9, Secondary=8

--- 3. Recovery and convergence ---
CONTROL Replication resumed
REPLICATE Secondary stock=9 version=2
READ     after recovery         Primary=9 (v2) | Secondary=9 (v2)
RESULT   Convergence successful: both replicas return 9
```

This demonstrates that the Primary receives updates immediately, the Secondary catches up during normal operation, a paused update produces a stale read, and resumed replication eventually makes both replicas agree again.

## Reflection

Reading from a nearby replica is usually faster and can reduce load on the Primary, but the replica may not have received the newest update yet. Reading from the Primary or using a consistency guarantee gives fresher data, but can increase latency and create more load. Therefore, applications must choose between lower read latency and stronger read consistency based on how important the latest value is. For inventory, a checkout operation may require a fresh read, while a casual stock display may accept a short delay.
