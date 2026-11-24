"""Small primary/secondary inventory replication simulation.

The secondary applies updates asynchronously through a queue. Replication can
be paused to make the stale-read window visible and resumed to recover.
"""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class Update:
    version: int
    stock: int


class Replica:
    def __init__(self, name: str, initial_stock: int) -> None:
        self.name = name
        self._stock = initial_stock
        self._version = 0
        self._lock = threading.Lock()

    def read(self) -> tuple[int, int]:
        with self._lock:
            return self._stock, self._version

    def apply(self, update: Update) -> None:
        with self._lock:
            self._stock = update.stock
            self._version = update.version


class ReplicatedInventory:
    def __init__(self, initial_stock: int = 10, replication_delay: float = 0.15) -> None:
        self.primary = Replica("Primary", initial_stock)
        self.secondary = Replica("Secondary", initial_stock)
        self.replication_delay = replication_delay
        self._updates: queue.Queue[Update | None] = queue.Queue()
        self._paused = threading.Event()
        self._paused.set()  # Replication starts enabled.
        self._stop = threading.Event()
        self._next_version = 0
        self._worker = threading.Thread(target=self._replication_worker, daemon=True)
        self._worker.start()

    def write_primary(self, stock: int) -> Update:
        self._next_version += 1
        update = Update(self._next_version, stock)
        self.primary.apply(update)
        print(f"WRITE  Primary   stock={stock} version={update.version}")
        self._updates.put(update)
        return update

    def pause_replication(self) -> None:
        self._paused.clear()
        print("CONTROL Replication paused")

    def resume_replication(self) -> None:
        self._paused.set()
        print("CONTROL Replication resumed")

    def wait_for_convergence(self, timeout: float = 3.0) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.primary.read() == self.secondary.read():
                return True
            time.sleep(0.01)
        return False

    def close(self) -> None:
        self._stop.set()
        self._paused.set()
        self._updates.put(None)
        self._worker.join(timeout=1)

    def _replication_worker(self) -> None:
        while not self._stop.is_set():
            update = self._updates.get()
            if update is None:
                return
            self._paused.wait()
            time.sleep(self.replication_delay)
            self.secondary.apply(update)
            print(f"REPLICATE Secondary stock={update.stock} version={update.version}")


def show_reads(system: ReplicatedInventory, label: str) -> None:
    primary_stock, primary_version = system.primary.read()
    secondary_stock, secondary_version = system.secondary.read()
    print(
        f"READ     {label:<22} Primary={primary_stock} (v{primary_version}) | "
        f"Secondary={secondary_stock} (v{secondary_version})"
    )


def main() -> None:
    system = ReplicatedInventory(initial_stock=10)
    try:
        print("=== Module 8: Replication & Consistency ===")
        show_reads(system, "initial state")

        print("\n--- 1. Normal replication ---")
        system.write_primary(8)
        if not system.wait_for_convergence():
            raise RuntimeError("Replicas did not converge during normal replication")
        show_reads(system, "after replication")
        print("RESULT   Normal replication successful: both replicas return 8")

        print("\n--- 2. Stale read test ---")
        system.pause_replication()
        system.write_primary(9)
        show_reads(system, "during replication pause")
        print("RESULT   Stale read observed: Primary=9, Secondary=8")

        print("\n--- 3. Recovery and convergence ---")
        system.resume_replication()
        if not system.wait_for_convergence():
            raise RuntimeError("Replicas did not converge after resuming replication")
        show_reads(system, "after recovery")
        print("RESULT   Convergence successful: both replicas return 9")
    finally:
        system.close()


if __name__ == "__main__":
    main()
