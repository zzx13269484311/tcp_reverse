import socket
import struct
import random
import sys
from datetime import datetime

LOG_FILE = "run_log.txt"


# direction:Client->Server||Server->Client
# pkt_type:报文类型1/2/3/4
# length:报文长度

def write_log(direction, pkt_type, length, extra=""):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {direction} | Type={pkt_type} | Len={length} | {extra}\n")


# 解决粘包
def recv_exact(sock, n):
    data = b""
    while len(data) < n:  # 只要收到的字节数<n就一直读
        packet = sock.recv(n - len(data))  # 还需要读多少字节
        if not packet:  # 如果读的是空数据
            raise ConnectionError("Socket closed unexpectedly")
        data += packet
    return data


def split_file_to_chunks(file_bytes, Lmin, Lmax, seed):
    random.seed(seed)
    chunks = []
    remain = len(file_bytes)
    while remain > 0:
        if remain <= Lmax:
            take = remain
        else:
            take = random.randint(Lmin, Lmax)
        chunks.append(take)
        remain -= take
    return chunks, len(chunks)


def main():
    server_ip = sys.argv[1]
    server_port = int(sys.argv[2])
    Lmin = int(sys.argv[3])
    Lmax = int(sys.argv[4])
    seed = int(sys.argv[5])
    src_filename = sys.argv[6]
    out_filename = sys.argv[7]

    with open(src_filename, "r", encoding="ascii") as f:
        src_bytes = f.read().encode("ascii")

    chunk_sizes, N = split_file_to_chunks(src_bytes, Lmin, Lmax, seed)
    write_log("Client系统", "文件分块计算", 0, f"N={N} chunks={chunk_sizes}")

    print("文件大小:", len(src_bytes))
    print("chunk_sizes:", chunk_sizes)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_ip, server_port))
    write_log("Client->Server", "TCP连接建立", 0, f"{server_ip}:{server_port}")

    blocks_received = []

    try:
        # Initialization Type=1
        init_pkt = struct.pack("!HI", 1, N)
        sock.sendall(init_pkt)  # 所有数据全部发完才继续往下执行
        write_log("Client->Server", "Initialization(1)", len(init_pkt), f"N={N}")

        # 接收 Agree Type=2
        agree_type = struct.unpack("!H", recv_exact(sock, 2))[0]
        write_log("Server->Client", "Agree(2)", 2, "收到初始化同意")
        if agree_type != 2:
            print("服务端拒绝初始化")
            return

        offset = 0
        for block_idx, blk_len in enumerate(chunk_sizes):
            blk_data = src_bytes[offset:offset + blk_len]
            offset += blk_len
            blk_str = blk_data.decode("ascii")

            # 发送 reverseRequest Type=3
            req_pkt = struct.pack("!HI", 3, blk_len) + blk_data
            sock.sendall(req_pkt)
            write_log("Client->Server", f"reverseRequest(3) 块{block_idx + 1}", len(req_pkt), f"原始:{blk_str}")

            # 接收 reverseAnswer Type=4
            ans_type = struct.unpack("!H", recv_exact(sock, 2))[0]
            ans_len = struct.unpack("!I", recv_exact(sock, 4))[0]
            reversed_bytes = recv_exact(sock, ans_len)
            reversed_str = reversed_bytes.decode("ascii")

            print(f"第{block_idx + 1}块:{reversed_str}")
            write_log("Server->Client", f"reverseAnswer(4) 块{block_idx + 1}", 6 + ans_len, f"反转文本:{reversed_str}")
            blocks_received.append(reversed_str)

        # 最终全文件反转
        full_reverse = ''.join(reversed(blocks_received))
        with open(out_filename, "w", encoding="ascii") as f:
            f.write(full_reverse)

        print(f"\n全部块处理完毕，反转文件已输出至 {out_filename}")
        write_log("Client系统", "任务完成", 0, f"完整反转文件写入 {out_filename}")

    except Exception as e:
        write_log("Client系统", "运行异常", 0, str(e))
        print(f"程序出错：{e}")
    finally:
        sock.close()
        write_log("Client->Server", "TCP连接关闭", 0, "客户端断开")


if __name__ == "__main__":
    main()
