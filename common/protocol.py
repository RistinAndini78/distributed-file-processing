import json
import struct

HEADER_SIZE = 4  # 4-byte unsigned int untuk panjang payload

def send_msg(sock, data_dict):
    """Mengirim dictionary sebagai JSON dengan header panjang pesan (4-byte big-endian)."""
    payload = json.dumps(data_dict).encode("utf-8")
    header = struct.pack("!I", len(payload))
    sock.sendall(header + payload)

def recv_msg(sock):
    """Menerima pesan JSON utuh berdasarkan header panjang 4-byte."""
    header = _recv_exact(sock, HEADER_SIZE)
    if not header:
        return None
    msg_len = struct.unpack("!I", header)[0]
    payload = _recv_exact(sock, msg_len)
    if not payload:
        return None
    return json.loads(payload.decode("utf-8"))

def _recv_exact(sock, num_bytes):
    """Memastikan seluruh byte diterima secara lengkap dari TCP stream."""
    chunks = []
    bytes_recd = 0
    while bytes_recd < num_bytes:
        chunk = sock.recv(min(num_bytes - bytes_recd, 65536))
        if not chunk:
            return None
        chunks.append(chunk)
        bytes_recd += len(chunk)
    return b"".join(chunks)