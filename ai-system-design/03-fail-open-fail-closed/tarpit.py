"""Two ways for a limiter's Redis to be down, without touching the real one:
a port nobody listens on (connections are refused), and a server that accepts
connections and never answers (the store is up but stuck)."""
import socket
import threading


def refused_url() -> str:
    """A redis:// URL for a local port that nothing is listening on."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return f"redis://127.0.0.1:{port}/0"


class Tarpit:
    def __enter__(self) -> "Tarpit":
        self._server = socket.create_server(("127.0.0.1", 0))
        self._held: list[socket.socket] = []
        self.url = f"redis://127.0.0.1:{self._server.getsockname()[1]}/0"
        threading.Thread(target=self._accept, daemon=True).start()
        return self

    def _accept(self) -> None:
        while True:
            try:
                conn, _ = self._server.accept()
            except OSError:              # the listening socket was closed
                return
            self._held.append(conn)      # keep it open and say nothing

    def __exit__(self, *exc) -> None:
        self._server.close()
        for conn in self._held:
            conn.close()
