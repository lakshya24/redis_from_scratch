# Uncomment this to pass the first stage
import socket


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept() # wait for client
    pong = "+PONG\r\n"
    with conn:
        print("establishing connection...")
        conn.sendall(pong.encode())

if __name__ == "__main__":
    main()
