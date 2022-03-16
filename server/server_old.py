import os
from socket import *

server_ip = "localhost"
server_port = 8001
s_sock = socket(AF_INET,SOCK_STREAM)
s_sock.bind((server_ip, server_port))
s_sock.listen(5)
print("Server is ready")

fv_map = {}

def replica_file(filename):
    data = []
    print(filename)
    if "RENAME" in filename:
        msg = filename
        h = filename.split('|')[3]
        data = eval(h)
       # data = p['replicas']
    elif "DELETE" in filename:
        msg = filename
        h = filename.split('|')[2]
        data = eval(h)
        # data = p['replicas']
    else:
        fname = filename.split('|')[0]
        f = open(fname, 'r')
        text = f.read()
        f.close()
        msg = "REPLICATE|" + fname + "|" + text + "|" + str(fv_map[fname])
        h = filename.split('|')[3]
        data = eval(h)

    for i in data:
        server_ip = i['server_name']
        port = i['portno']
        try:
            replicate_socket = socket(AF_INET, SOCK_STREAM)
            replicate_socket.connect((server_ip, port))
            replicate_socket.send(msg.encode())
            print("replication done succesfully")
            replicate_socket.close()
        except error as msg:
            print(msg)
            print("exception occured")


def read_write_request(filename, rw, write_data, fv_map):
    if rw == "a+":  # if write request
        if filename not in fv_map:
            fv_map[filename] = 0  # if empty (ie. if its a new file), set the version no. to 0
        else:
            fv_map[filename] += 1  # increment version no.

        file = open(filename, "a+")
        file.write(write_data)
        print("New version of " + filename + " is " + str(fv_map[filename]))
        return "write request is successful", fv_map[filename]

    elif rw == "r":  # if read request
        try:
            file = open(filename, rw)
            filedata = file.read()  # read the file's text into a string
            if filename not in fv_map:
                fv_map[filename] = 0
            return (filedata, fv_map[filename])
        except IOError:  # IOError occurs when open(filepath,RW) cannot find the file requested
            print(filename + " does not exist\n")
            return "File does not exist", -1

def client_response(resp, rw, client_socket):
    if resp[0] == "write request is successful":
        reply = "File successfully written to..." + str(resp[1])
    elif resp[0]!="File does not exist" and rw == "r":
        reply=resp
    elif resp[0] is IOError:
        reply = "File does not exist\n"
    client_socket.send(reply.encode())


def main():
    while 1:
        client_socket, address = s_sock.accept()
        msg= client_socket.recv(1024)
        msg= msg.decode()
        # print(msg)
        if "RENAME" in msg:
            oldfilename = msg.split("|")[0]
            newfilename = msg.split("|")[1]
            os.rename(oldfilename, newfilename)
            hi = "success"
            replica_file(msg)
            client_socket.send(hi.encode())
        elif "DELETE" in msg:
            hi = ""
            file = msg.split("|")[0]
            if os.path.exists(file):
                os.remove(file)
                hi="success"
            replica_file(msg)
            client_socket.send(hi.encode())
        elif msg != "" and "CHECK_VERSION" not in msg:
            filename,rw,text, temp = msg.split("|")  # file path to perform read/write on
            response = read_write_request(filename, rw, text, fv_map)  # perform the read/write and check if successful
            client_response(response, rw, client_socket)  # send back write successful message or send back text for client to read
            if rw == 'a+':
                replica_file(msg)
        elif "CHECK_VERSION" in msg:
            c_file = msg.split("|")[1]  # parse the version number to check
            print("Checking version of " + c_file)
            client_socket.send(str(fv_map[c_file]).encode())
    client_socket.close()

if __name__ == "__main__":
        main()

