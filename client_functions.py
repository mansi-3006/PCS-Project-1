import time
from socket import *
import sys
import os.path

ds_ip = "localhost"
ds_port =9001
lock_ip = 'localhost'
lock_port = 8051
curr_path = os.path.dirname(os.path.realpath(sys.argv[0]))
def socket_connection():
    c_sock = socket(AF_INET, SOCK_STREAM)
    return c_sock


def check_cache(filename_DS, file, c_id, flag):
    cache_filepath = curr_path + "\\client_cache" + c_id + "\\" + file  # append the cache folder and filename to the path
    if flag == 0:
        if os.path.exists(cache_filepath):
            new_filepath = curr_path + "\\client_cache" + c_id + "\\" + filename_DS
            os.rename(cache_filepath, new_filepath)
    else:
        if os.path.exists(cache_filepath):
            os.remove(cache_filepath)

def send_rename(c_sock,file,filename_DS,IP_DS,PORT_DS,c_id,replicate_servers):
    file+=".txt"
    send_msg =  file+ "|" + filename_DS+ "|" + "RENAME"+ "|"+ replicate_servers
    check_cache(filename_DS,file,c_id,0)
    c_sock.connect((IP_DS,PORT_DS))
    c_sock.send(send_msg.encode())
    resp = c_sock.recv(1024)
    if resp is not None:
        print("rename was success")

def send_delete(c_sock,file,filename_DS,IP_DS,PORT_DS,c_id,replicate_servers):
    msg = filename_DS+ "|" + "DELETE"+"|"+replicate_servers
    check_cache(filename_DS,file,c_id,1)
    c_sock.connect((IP_DS,PORT_DS))
    c_sock.send(msg.encode())
    resp = c_sock.recv(1024)
    if resp is not None:
        print("delete was success")

def write(c_sock, fs_IP, fs_PORT, filename , rw, fvmap, msg,replicate_servers):
    if filename not in fvmap:
        fvmap[filename] = 0
    elif rw != "r":
        fvmap[filename] += 1

    send_query = filename + "|" + rw + "|" + msg + "|" + replicate_servers
    # send the sting requesting a write to the file server
    c_sock.connect((fs_IP,fs_PORT))
    c_sock.send(send_query.encode())

def read(c_sock, fs_IP, fs_PORT, filename , rw, fvmap, msg, filename_DS, c_id):

    if filename not in fvmap:
        print("Requested file is not in cache")
        send_query = filename + "|" + rw + "|" + msg
        try:
            c_sock.connect((fs_IP,fs_PORT))
            c_sock.send(send_query.encode())
            fvmap[filename] = 0
            return False
        except error as msg:
            return 2
    cache_filepath = curr_path + "\\client_cache" + c_id + "\\" + filename_DS
    if os.path.exists(cache_filepath) == True:
        send_query = "CHECK_VERSION|" + filename
        cs1 = socket_connection()
        try:
            cs1.connect((fs_IP, fs_PORT))
            cs1.send(send_query.encode())
            fsversion = cs1.recv(1024)    # receive file server version number
            fsversion = fsversion.decode()
        except error as e:
            print("Not getting connected")
            return 2
        cs1.close()

    if fsversion != str(fvmap[filename]):
        print("Versions are different and so requesting file from server")
        fvmap[filename] = int(fsversion)
        send_query = filename + "|" + rw + "|" + msg

        # send the string requesting a read from the file server
        c_sock.connect((fs_IP,fs_PORT))
        c_sock.send(send_query.encode())
        return False    # didn't go to cache - new version
    else:
        # read from cache
        print("Reading from cache as the file is updated")
        cache(filename_DS, "READ", "r", c_id)

    return True     # went to cache

def cache(filename_DS,text,rw,c_id):
    cache_filepath = curr_path + "\\client_cache" + c_id + "\\" + filename_DS  # append the cache folder and filename to the path

    os.makedirs(os.path.dirname(cache_filepath), exist_ok=True)  # create the directory/file
    if rw == "a+" or rw == "w":
        with open(cache_filepath, rw) as f:  # write to the cached file
            f.write(text)

    else:
        with open(cache_filepath, "r") as f:  # read from the cached file
            print("--------------------------------")
            print(f.read())
            print("--------------------------------")
    return False

def menu():
    print("\n----------------MENU---------------")
    print("<list> - List all existing files")
    print("<create> [filename] [permission]- Create the file")
    print("\t\t\tchoose the permission from below: 1 -> read_only, 2 -> read_write, 3 -> restricted")
    print("<write> [Filename] - Write text to a file")
    print("<read> [Filename] - Read from a file")
    print("<rename> [oldfilename] [newfilename] - Rename the file")
    print("<delete> [filename] - Delete the file")
    #print("<permission>- lists all permission types")
   # print("<menu> - Main Menu")
    print("<quit> - Quit from the application")
    print("-------------------------------------")

def directory(c_sock, c_id, filename, rw, flag, check_list, atype = " "):
    c_sock.connect((ds_ip,ds_port))
    if check_list:
        msg = " " + '|' + " " + '|' + "LIST" + '|' + " "
        c_sock.send(msg.encode())
        resp = c_sock.recv(1024).decode()
        c_sock.close()
        print("List of existing files:\n")
        print(resp)
    else:
        if flag==0:
            msg = filename+ '|' + rw + '|' +"CREATE"+ '|' +c_id+ '|' + atype
            print(msg)
        elif len(filename.split())==2:
             msg = filename + '|' + rw + '|' +"RENAME"+ '|' +c_id
        #elif flag==1:
        #    msg = filename + '|' + rw + '|' + "Write" + '|' + c_id
        elif flag==2:
            msg = filename + '|' + rw + '|' +"DELETE"+ '|' +c_id
        else:
            msg = filename + '|' + rw + '|' +"Read"+'|'+c_id
        # send the string requesting file info to directory send_directory_service
        c_sock.send(msg.encode())
        resp = c_sock.recv(1024).decode()
    return resp

def validity(input, rename = 0):
    if len(input.split()) <2:
        print("Format is incorrect")
        menu()
        return False
    elif len(input.split()) < 3 and rename == 1:
        print("Format is incorrect")
        menu()
        return False
    else:
        return True


def lock_unlock_file(c_sock, c_id, filename, flag):
    c_sock.connect((lock_ip, lock_port))
    if flag == "lock":
        #msg = filename + "|" + "_1_:" + "| |" + c_id
        msg = c_id + "|_1_:|" + filename  # 1 = lock the file
    elif flag == "unlock":
        #msg = filename + "|" + "_2_:" + "| |" + c_id
        msg = c_id + "|_2_:|" + filename  # 2 = unlock the file

    # send the string requesting file info to directory service
    c_sock.send(msg.encode())
    resp = c_sock.recv(1024).decode()
    return resp

def handle_create(filename,atype,c_id, flag,fv_map):
    #file creation started
    c_sock = socket_connection()  # create socket to directory service
    reply = directory(c_sock, c_id, filename, 'w',flag, False,atype)  # request the file info from directory service
    c_sock.close()   # close the connection
    if reply == "FILE_Already_EXIST":
        print(filename + " already exist on a fileserver")
        return False
    else:
        print(reply)
        filename_DS, IP_DS, PORT_DS, replicate_servers = reply.split('|')
        execute_write(filename_DS,IP_DS,PORT_DS, filename, c_id, fv_map, replicate_servers)
    return True

def execute_write(filename_DS, IP_DS, PORT_DS, filename, c_id, fv_map, replicate_servers):
    # ------ LOCKING ------
    c_sock = socket_connection()
    grant_lock = lock_unlock_file(c_sock, c_id, filename_DS, "lock")
    c_sock.close()
    while grant_lock != "file_granted":
        print("File not granted, polling again...")
        c_sock = socket_connection()
        grant_lock = lock_unlock_file(c_sock, c_id, filename_DS, "lock")
        c_sock.close()

        if grant_lock == "TIMEOUT":     # if timeout message received from locking service, break
            return False
        time.sleep(0.1)     # wait 0.1 sec if lock not available and request it again

    print("You are granted the file...")
    # ------ ClIENT WRITING TEXT ------
    print("Write some text...")
    print("<end> to finish writing")
    print("-----------------------------------")
    text = ""
    while True:
        input = sys.stdin.readline()
        if "<end>" in input:  # check if user wants to finish writing
            break
        else:
            text += input
    print("-----------------------------------")
    # ------ WRITING TO FS ------
    c_sock = socket_connection()
    write(c_sock, IP_DS, int(PORT_DS), filename_DS, "a+", fv_map, text, replicate_servers) # send text and filename to the fileserver
    #print ("SENT FOR WRITE")
    resp = c_sock.recv(1024).decode()
    c_sock.close()
    print (resp.split("...")[0])    # split version num from success message and print message
    version_num = int(resp.split("...")[1])
    if version_num != fv_map[filename_DS]:
        print("Server version no changed - updating client version no.")
        fv_map[filename_DS] = version_num
    # ------ CACHING ------
    cache(filename_DS, text, "a+", c_id)

    # ------ UNLOCKING ------
    c_sock = socket_connection()
    reply_unlock = lock_unlock_file(c_sock, c_id, filename_DS, "unlock")
    c_sock.close()
    print (reply_unlock)
    return True

def handle_write(filename, c_id, flag, fv_map):
    # ------ INFO FROM DS ------
    c_sock = socket_connection()  # create socket to directory service
    resp = directory(c_sock, c_id, filename, 'w', flag, False)  # request the file info from directory service
    c_sock.close()  # close the connection
    if resp == "FILE_DOES_NOT_EXIST":
        print(filename + " does not exist on a fileserver")
    else:
        filename_DS, IP_DS, PORT_DS, replicate_servers = resp.split('|')
        execute_write(filename_DS, IP_DS,PORT_DS, filename, c_id, fv_map, replicate_servers)
        return True

def handle_read(filename, fv_map, c_id):
    c_sock = socket_connection()  # create socket to directory service
    resp = directory(c_sock, c_id, filename, 'r', 1, False)  # send file name to directory service
    c_sock.close()   # close directory service connection
    if resp == "FILE_DOES_NOT_EXIST":
        print(filename + " does not exist on a fileserver")
    else:
        # parse info received from the directory service
        filename_DS=resp.split('|')[0]
        data = resp.split('|')[1]
        p = eval(data)
        h = p['replicas']
        for i in h:
            IP_DS = i['server_name']
            PORT_DS = i['portno']
            c_sock = socket_connection()  # create socket to file server

            read_cache = read(c_sock, IP_DS, int(PORT_DS), filename_DS, "r", fv_map, "READ", filename_DS, c_id) # send filepath and read to file server
            if read_cache != 2:
                break

        if not read_cache:
            reply = c_sock.recv(1024).decode()    # receive reply from file server, this will be the text from the file
            c_sock.close()

            if reply != "EMPTY_FILE":
                print("-------------------------------------------")
                print (reply)
                print("-------------------------------------------")
                cache(filename_DS, reply, "w", c_id)  # update the cached file with the new version from the file server
                print (filename_DS + " successfully cached...")
            else:
                print(filename_DS + " is empty...")
                del fv_map[filename_DS]

def handle_rename(oldfilename,newfilename,c_id):
    c_sock = socket_connection() # create socket to directory service
    filename=oldfilename+" "+newfilename
    resp = directory(c_sock, c_id, filename, 'r', 1, False)  # send file name to directory service
    c_sock.close()   # close directory service connection
    if resp == "FILE_DOES_NOT_EXIST":
        print("you don't have access to rename this file or this file is not present in your system")
        return False
    else:
        c_sock = socket_connection()
        filename_DS, IP_DS, PORT_DS, replicate_servers = resp.split('|')
        send_rename(c_sock, oldfilename, filename_DS, IP_DS, int(PORT_DS), c_id, replicate_servers)
        return True

def handle_delete(file, c_id):
    c_sock = socket_connection()  # create socket to directory service
    resp = directory(c_sock, c_id, file, 'r', 2, False)  # send file name to directory service
    c_sock.close()   # close directory service connection
    if resp == "FILE_DOES_NOT_EXIST":
        print("you don't have access to delete this file or this file is not present in your system")
        return False
    else:
        c_sock = socket_connection()
        p=eval(resp)
        filename_DS = p[file]['filename']
        IP_DS =(p[file]['primary']['server_name'])
        #IP_DS = 'localhost'
        PORT_DS = p[file]['primary']['portno']
        replicate_servers=str(p[file]['replicas'])

        #-----------------------LOCKING------------------------
        grant_lock = lock_unlock_file(c_sock, c_id, filename_DS, "lock")
        c_sock.close()
        while grant_lock != "file_granted":
            print("File not granted, polling again...")
            c_sock = socket_connection()
            grant_lock = lock_unlock_file(c_sock, c_id, filename_DS, "lock")
            c_sock.close()

            if grant_lock == "TIMEOUT":     # if timeout message received from locking service, break
                return False
            time.sleep(0.1)     # wait 0.1 sec if lock not available and request it again

        print("You are granted the file...")     

        c_sock = socket_connection()
        send_delete(c_sock, file, filename_DS, IP_DS, int(PORT_DS), c_id, replicate_servers)
        c_sock.close()

        # ------ UNLOCKING ------
        c_sock = socket_connection()
        reply_unlock = lock_unlock_file(c_sock, c_id, filename_DS, "unlock")
        c_sock.close()
        print (reply_unlock)
        cache_filepath = curr_path + "\\client_cache" + c_id + "\\" + filename_DS
        if os.path.exists(cache_filepath):
            os.remove(cache_filepath)


        return True
