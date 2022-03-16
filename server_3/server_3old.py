import os
from socket import *

server_ip = "localhost"
server_port = 8003
s_sock = socket(AF_INET,SOCK_STREAM)
s_sock.bind((server_ip, server_port))
s_sock.listen(5)
print("Server is ready")

fv_map = {}

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
    elif resp[1]!=-1 and rw == "r":
        reply=resp[0]
    elif resp[1] == -1 :
        reply = resp[0]
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
            response = os.rename(oldfilename, newfilename)
        elif "DELETE" in msg:
            file = msg.split("|")[0]
            if os.path.exists(file):
                os.remove(file)
        elif msg != "" and "CHECK_VERSION" not in msg  and "REPLICATE" not in msg:
            filename,rw,text = msg.split("|")  # file path to perform read/write on
            response = read_write_request(filename, rw, text, fv_map)  # perform the read/write and check if successful
            client_response(response, rw, client_socket)  # send back write successful message or send back text for client to read
        elif "CHECK_VERSION" in msg:
            c_file = msg.split("|")[1]  # parse the version number to check
            print("Checking version of " + c_file + "\n")
            if c_file not in fv_map:
                fv_map[c_file] = 0
            file_version = str(fv_map[c_file])
            client_socket.send(file_version.encode())
        elif "REPLICATE|" in msg:
            temp, rep_filename, rep_text, rep_version = msg.split("|")
            fv_map[rep_filename] = int(rep_version)
            f = open(rep_filename, 'w')
            f.write(rep_text)
            f.close()
            print(rep_filename + " successfully replicated...\n")
    client_socket.close()

if __name__ == "__main__":
        main()
