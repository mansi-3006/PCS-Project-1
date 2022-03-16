from socket import *
from collections import defaultdict  # for dictionary list

lock_ip = 'localhost'
lock_port = 8051
s_sock = socket(AF_INET, SOCK_STREAM)
s_sock.bind((lock_ip, lock_port))
s_sock.listen(10)
print('Locking service is ready')


def check_unlock(filename, locked_map):
    if filename in locked_map:  # check for existance of filename as a key in the dictionary
        if locked_map[filename] == "unlocked":
            return True
        else:
            return False
    else:
        locked_map[filename] = "unlocked"
        return True


def main():
    locked_map = {}
    clients_map = defaultdict(list)
    waiting_client = False
    client_timeout_map = {}

    while 1:
        c_sock, address = s_sock.accept()
        msg = c_sock.recv(1024).decode()
        print("\nRECEIVED: " + msg)

        if "_1_:" in msg:
            c_id,filename = msg.split("_1_:")
            clientwaiting_flag = False

            unlocked = check_unlock(filename, locked_map)
            if unlocked == True:
                count_temp = 0  # a count to check if current client is first in the queue

                if len(clients_map[filename]) == 0:  # if no clients currently waiting on the file
                    locked_map[filename] = "locked"  # lock the file
                    grant_message = "file_granted"
                    print("SENT: " + grant_message + " ---- " + c_id)
                    c_sock.send(grant_message.encode())  # send the grant message

                elif filename in clients_map:
                    for filename, values in clients_map.items():  # find the current file in the map
                        for v in values:  # iterate though the clients waiting on this file
                            if v == c_id and count_temp == 0:  # if the client is the first client waiting
                                clients_map[filename].remove(v)  # remove it from the waiting list
                                locked_map[filename] = "locked"  # lock the file
                                grant_message = "file_granted"
                                print("SENT: " + grant_message + " ---- " + c_id)
                                c_sock.send(grant_message.encode())  # send the grant message
                            count_temp += 1
            else:  # if the file is locked
                grant_message = "file_not_granted"

                if c_id in client_timeout_map:  # check if first time requesting file
                    client_timeout_map[c_id] = client_timeout_map[c_id] + 1  # if first time, set timeout value to 0
                    print("TIME: " + str(client_timeout_map[c_id]))
                else:
                    client_timeout_map[c_id] = 0  # if not first time, increment timeout value of client

                if client_timeout_map[c_id] == 100:  # if client polled 100 times (10 sec), send timeout
                    timeout_msg = "TIMEOUT"
                    for filename, values in clients_map.items():  # find the current file in the map
                        for v in values:  # iterate though the clients waiting on this file
                            if v == c_id:  # if the client is the first client waiting
                                clients_map[filename].remove(v)  # remove it from the waiting list
                    del client_timeout_map[c_id]  # remove client from timeout map
                    c_sock.send(timeout_msg.encode())  # send timeout msg
                else:

                    if filename in clients_map:
                        for filename, values in clients_map.items():  # find the current file in the map
                            for v in values:  # iterate though the clients waiting on this file
                                if v == c_id:  # check if client is already waiting on the file
                                    clientwaiting_flag = True  # if already waiting, set flag - so client is not added to waiting list multiple times for the file

                    if clientwaiting_flag == False:  # if not already waiting
                        clients_map[filename].append(c_id)  # append client to lists of clients waiting for the file

                    print("SENT: " + grant_message + c_id)
                    c_sock.send(grant_message.encode())  # send file not granted message

        elif "_2_:" in msg:  # if unlock message (_2_) received
            c_id,filename = msg.split("_2_:")
            locked_map[filename] = "unlocked"  # unlock the current file
            grant_message = "File unlocked..."
            c_sock.send(grant_message.encode())  # tell the current client that the file was unlocked

        c_sock.close()

if __name__ == "__main__":
    main()