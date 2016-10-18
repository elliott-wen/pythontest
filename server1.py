import socket
import time
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("10.0.0.1",8080))
while True:
    r,d = sock.recvfrom(1024)
    print r