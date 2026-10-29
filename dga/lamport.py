"""Relógio lógico de Lamport (thread-safe).

Regras (Lamport, 1978):
  * Antes de um evento local ou de enviar mensagem:  L = L + 1
  * Ao receber mensagem com timestamp t:             L = max(L, t) + 1
"""
import threading


class LamportClock:
    def __init__(self) -> None:
        self._t = 0
        self._lock = threading.Lock()

    def tick(self) -> int:
        """Evento local / envio. Retorna o novo valor do relógio."""
        with self._lock:
            self._t += 1
            return self._t

    def update(self, received: int) -> int:
        """Recebimento de mensagem com timestamp `received`."""
        with self._lock:
            self._t = max(self._t, int(received)) + 1
            return self._t

    @property
    def time(self) -> int:
        with self._lock:
            return self._t
