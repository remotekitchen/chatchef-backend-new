# billing/jobrunner.py
import threading

class SingleJobRunner:
    def __init__(self):
        self._lock = threading.Lock()
        self._running = False

    def start(self, target):
        """Start target() in a daemon thread. Returns False if already running."""
        with self._lock:
            if self._running:
                return False
            self._running = True

        def _run():
            try:
                target()
            finally:
                with self._lock:
                    self._running = False

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        return True

    def is_running(self):
        with self._lock:
            return self._running

# Singleton for HT sync
ht_sync_runner = SingleJobRunner()
