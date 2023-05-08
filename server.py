


from pysondb.pysondb_server import ClientTCPHandler,SocketServer
from pysondb.config import Config 


def main() -> int:
    server = SocketServer() 
    server.serve_forever()
    return 0

if __name__ == "__main__":
    main()
