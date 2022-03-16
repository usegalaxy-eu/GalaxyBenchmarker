#!/usr/bin/python3
import sys
import socket
import time

def server(host, port, num_clients):
    """Start a server which waits for num_clients connections and syncs the clients

    When num_clients connections connected, all will receive "go!"
    """
    start = time.monotonic()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(20.0)
        s.bind((host, port))
        s.listen(num_clients)
        print(f"Server started. Accepting clients.")

        # Wait for all clients to connect
        connections = []
        for i in range(num_clients):
            connections.append(s.accept())
            print(f"{i} connected after {time.monotonic() - start}")

        # All clients connected and are waiting
        for conn, _ in connections:
            conn.sendall(b'go!')
            conn.close()

    print(f"Server stopped after {time.monotonic() - start}")

def client(host, port):
    """Connect to the sync server and wait for the 'go!' signal"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(20.0)
        s.connect((host, port))
        print("Client waiting")

        s.recv(3)
        print("Client running")

if __name__ == "__main__":
    """Usage:
    Start Server: [host] [port] [num_clients]
    Start Client: [host] [port]
    """
    print(sys.argv)
    if len(sys.argv) == 4:
        _, host, port, num_clients = sys.argv
        server(host, int(port), int(num_clients))
    elif len(sys.argv) == 3:
        _, host, port = sys.argv
        client(host, int(port))
    else:
        print("Invalid number of arguments")
        sys.exit(1)
