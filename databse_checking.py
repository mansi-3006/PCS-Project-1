import json
from socket import *

ds_ip = '192.168.196.129'
ds_port = 9001
s1_ip = '192.168.196.129'
s2_ip = '192.168.196.169'
s3_ip ='192.168.196.37'
s_sock = socket(AF_INET, SOCK_STREAM)
s_sock.bind((ds_ip, ds_port))
s_sock.listen(5)


def get_permission(i):
    m = int(i)
    if m == 1:
        return "read_only"
    elif m == 2:
        return "read_write"
    elif m == 3:
        return "Restricted"


def create_mappings(client_msg):
    filename, t1, t2, author, permission = client_msg.split('|')
    print(filename)
    permission1=get_permission(permission)
    print(permission1)
    with open("filemappings.json", 'r') as file:
        j = json.load(file)
        if filename in j:
            return None
        else:
            print("entered the creation of json block")
            data = {}
            data[filename] = {}
            data[filename]['filename'] = filename + ".txt"
            data[filename]['author'] = author
            data[filename]['primary'] = {}
            data[filename]['primary']['server_name'] = s1_ip
            data[filename]['primary']['portno'] = 8001
            data[filename]['replicas'] = []
            data2 = {}
            data3 = {}
            data2['server_name'] = s2_ip
            data3['server_name'] = s3_ip
            data2['portno'] = 8002
            data3['portno'] = 8003

            data[filename]['replicas'].append(data2)
            data[filename]['replicas'].append(data3)
            data[filename]['permission'] = permission1
            j.update(data)
            with open("filemappings.json", 'w') as file:
                print(json.dump(j, file, indent=4))
            return "file mapped successfully"


def check_mappings(client_msg, list_files, flag=0):
    print("hiiiiiiiiiiiiiiiiii ",client_msg)
    filename = client_msg.split('|')[0]
    rw = client_msg.split('|')[1]
    if rw == 'w' or rw == 'a+':
        m = "read_write"
    else:
        m = "read_only"
    uname = client_msg.split('|')[3]
    with open("filemappings.json", 'rt') as file:
        j = json.load(file)  # open the .csv file storing the mappings 	# skip header of csv file
        file_row = ""
        data = {}
        for key, i in j.items():
            if not list_files:
                print(key)
                print(filename)
                print(m)
                print(uname)
                if key == filename and rw == 'w':
                    print(i['author'])
                    print(i['permission'])
                    if i['permission'] == m:
                        print('WRITING')
                        actual_filename = i['filename']  # get actual filename (eg. file123.txt)
                        server_addr = str(i['primary']['server_name'])  # get the file's file server IP address
                        server_port = str(i['primary']['portno'])  # get the file's file server PORT number
                        replicas = i['replicas']
                        print("actual_filename: " + actual_filename)
                        print("server_addr: " + server_addr)
                        print("server_port: " + server_port)

                        return actual_filename + "|" + server_addr + "|" + server_port + "|" + str(replicas)

                    if uname == i['author']:
                        print('WRITING')
                        actual_filename = i['filename']  # get actual filename (eg. file123.txt)
                        server_addr = str(i['primary']['server_name'])  # get the file's file server IP address
                        server_port = str(i['primary']['portno'])  # get the file's file server PORT number
                        replicas = i['replicas']
                        print("actual_filename: " + actual_filename)
                        print("server_addr: " + server_addr)
                        print("server_port: " + server_port)
                        return actual_filename + "|" + server_addr + "|" + server_port + "|" + str(replicas)

                elif key == filename and rw == 'r':
                    if i['permission']!="Restricted" :
                        print('Reading')
                        actual_filename = i['filename']  # get actual filename (eg. file123.txt)
                        data['replicas'] = i['replicas']
                        print("actual_filename: " + actual_filename)
                        print("server_data: " + str(data))

                        return actual_filename + "|" + str(data)
                    if uname == i['author']:
                        print('Reading')
                        actual_filename = i['filename']  # get actual filename (eg. file123.txt)
                        data['replicas'] = i['replicas']
                        print("actual_filename: " + actual_filename)
                        print("server_data: " + str(data))
                        return actual_filename + "|" + str(data)
            else:
                file_row = file_row + key + "\n"
        if list_files == True:
            return file_row
    return None  # if file does not exist return None


def delete_mappings(client_msg):
    key = client_msg.split('|')[0]
    data = {}
    with open("filemappings.json", 'rt') as file:
        j = json.load(file)
        author = client_msg.split('|')[3]
        if key in j:
            data[key] = j[key]
        else:
            return None
        if j[key]['permission'] == "read_write":
            del j[key]
        elif j[key]['permission'] == "Restricted" and author == j[key]['author']:
            del j[key]
        else:
            return None
        with open("filemappings.json", 'w') as file:
            print(json.dump(j, file, indent=4))
    return data


def change_mappings(client_msg):
    filename = client_msg.split('|')[0]
    file1 = filename.split()[0]
    file2 = filename.split()[1]
    author = client_msg.split('|')[3]
    data = {}
    c = 0
    with open("filemappings.json", 'rt') as file:
        j = json.load(file)
        for key, i in j.items():
            if key == file1:
                if j[key]['permission'] == 'read_write':
                    data[file2] = j[key]
                    data[file2]['filename'] = file2 + ".txt"
                    c = 1
                elif j[key]['permission'] == 'Restricted' and j['key']['author'] == author:
                    data[file2] = j[key]
                    data[file2]['filename'] = file2 + ".txt"
                    c = 1
                else:
                    return None

            if c == 1:
                del j[key]
                break
        j.update(data)
        with open("filemappings.json", 'w') as file:
            print(json.dump(j, file, indent=4))
    if c == 1:
        actual_filename = data[file2]['filename']  # get actual filename (eg. file123.txt)
        server_addr = str(data[file2]['primary']['server_name'])  # get the file's file server IP address
        server_port = str(data[file2]['primary']['portno'])  # get the file's file server PORT number
        replicas = data[file2]['replicas']
        print("actual_filename: " + actual_filename)
        print("server_addr: " + server_addr)
        print("server_port: " + server_port)
        return actual_filename + "|" + server_addr + "|" + server_port + "|" + str(replicas)
    else:
        return None


def main():
    while 1:
        c_sock, addr = s_sock.accept()

        response = ""
        recv_msg = c_sock.recv(1024)
        recv_msg = recv_msg.decode()
        print("++++++++++++++",recv_msg)
        if "CREATE" in recv_msg:
            print("started the mapping scenario")
            response1 = create_mappings(recv_msg)
            if response1 is not None:
                response = check_mappings(recv_msg, False, 1)
            else:
                response = "FILE_Already_EXIST"
                print("RESPONSE: \n" + response)
                print("\n")
        elif "LIST" in recv_msg:
            print("entered the dummy block")
            response = check_mappings(recv_msg, True)  # check the mappings for the file
        elif "RENAME" in recv_msg:
            response = change_mappings(recv_msg)
        elif "DELETE" in recv_msg:
            response = delete_mappings(recv_msg)
        else:
            response = check_mappings(recv_msg, False)
        if response is not None:  # for existance of file
            response = str(response)
            print("RESPONSE: \n" + response)
            print("\n")
        else:
            response = "FILE_DOES_NOT_EXIST"
            print("RESPONSE: \n" + response)
            print("\n")
        c_sock.send(response.encode())  # send the file information or non-existance message to the client
        c_sock.close()


if __name__ == "__main__":
    main()
