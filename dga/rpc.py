"""Utilitários de RPC (XML-RPC da biblioteca padrão do Python).

Todas as chamadas do sistema têm a assinatura  metodo(remetente, timestamp, *args)
e retornam  {"ts": <lamport do respondente>, "result": <valor>}.
"""
import xmlrpc.client
from socketserver import ThreadingMixIn
from xmlrpc.server import SimpleXMLRPCRequestHandler, SimpleXMLRPCServer


class TimeoutTransport(xmlrpc.client.Transport):
    """Transport do XML-RPC com timeout de conexão/leitura."""

    def __init__(self, timeout: float) -> None:
        super().__init__()
        self._timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self._timeout
        return conn


class QuietHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ("/RPC2",)

    def log_message(self, *args):  # silencia o log HTTP padrão
        pass


class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    """Servidor multithread: cada requisição é atendida em sua própria thread."""

    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 64
