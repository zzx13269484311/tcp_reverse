import socket
import threading
import struct
from datetime import datetime

LOG_FILE = "run_log.txt"


def write_log(direction, pkt_type, length, extra=""):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {direction} | Type={pkt_type} | Len={length} | {extra}\n")


def recv_exact(sock, n):
    data = b""
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            raise ConnectionError("Socket closed unexpectedly")
        data += packet
    return data


def handle_client(conn, addr):
    print(f"新客户端连接: {addr}")
    write_log(f"Server<-Client({addr})", "连接建立", 0, f"客户端地址{addr}")
    try:
        # Initialization Type=1
        header = recv_exact(conn, 2)
        pkt_type = struct.unpack("!H", header)[0]
        if pkt_type != 1:
            write_log(f"Server<-Client({addr})", "错误报文", 2, f"预期Type1,收到{pkt_type}")
            return

        n_raw = recv_exact(conn, 4)
        N = struct.unpack("!I", n_raw)[0]
        write_log(f"Server<-Client({addr})", "Initialization(1)", 6, f"N={N}")

        # 回复 Agree Type=2
        agree_pkt = struct.pack("!H", 2)
        conn.sendall(agree_pkt)
        write_log(f"Server->Client({addr})", "Agree(2)", len(agree_pkt), "同意初始化")

        # 处理 N 块 reverseRequest
        blocks = []
        for block_idx in range(1, N + 1):
            req_header = recv_exact(conn, 2)
            req_type = struct.unpack("!H", req_header)[0]
            if req_type != 3:
                write_log(f"Server<-Client({addr})", "异常报文", 2, f"块{block_idx}预期Type3,收到{req_type}")
                break

            len_raw = recv_exact(conn, 4)
            data_len = struct.unpack("!I", len_raw)[0]
            data = recv_exact(conn, data_len)
            text = data.decode("ascii")
            write_log(f"Server<-Client({addr})", f"reverseRequest(3) 块{block_idx}", 6 + data_len, f"原始文本:{text}")

            # 反转文本
            reversed_text = text[::-1]
            reversed_bytes = reversed_text.encode("ascii")
            ans_pkt = struct.pack("!HI", 4, len(reversed_bytes)) + reversed_bytes
            conn.sendall(ans_pkt)
            write_log(f"Server->Client({addr})", f"reverseAnswer(4) 块{block_idx}", len(ans_pkt), f"反转文本:{reversed_text}")
            blocks.append(reversed_text)

    except Exception as e:
        write_log(f"Server({addr})", "会话异常", 0, str(e))
    finally:
        conn.close()
        write_log(f"Server({addr})", "连接关闭", 0, "会话结束")
        print(f"客户端 {addr} 连接已关闭")


def start_server(port=8888):
    open(LOG_FILE, "w", encoding="utf-8").close()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", port))
    s.listen(5)
    print(f"服务端启动，监听0.0.0.0:{port}")
    write_log("Server系统", "系统启动", 0, f"监听端口{port}")

    while True:
        conn, addr = s.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        t.start()
        print(f"当前活跃连接数: {threading.active_count()-1}")


if __name__ == "__main__":
    start_server(8888)