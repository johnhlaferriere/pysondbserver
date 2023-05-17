


from pysondb.pysondb_server import ClientTCPHandler,SocketServer


def main() -> int:
    try:
        server = SocketServer() 
        server.serve_forever()
    except:
        pass
    return 0

if __name__ == "__main__":
    main()
