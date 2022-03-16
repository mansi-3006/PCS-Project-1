import csv
import sys
import getpass
import time
from builtins import input

import client_functions


def user_creation():
    print("enter the username:")
    username = input()
    while 1:
        pwd = getpass.getpass("enter the password:")
        pwd1 = getpass.getpass("please re-enter the password again:")
        if pwd == pwd1:
            break
        else:
            print("password didn't match so please retype your password")
    details = []
    details.append(username)
    details.append(pwd)
    with open("logins.csv", 'a+', newline='') as login:
        writer = csv.writer(login)
        writer.writerow(details)
        login.close()
    return True


def user_verify():
    while 1:
        print("please enter the user name:")
        username = input()
        j = 0
        pwd = getpass.getpass("enter the password:")
        with open("logins.csv", 'rt') as file:
            d_reader = csv.DictReader(file, delimiter=',')  # read file as a csv file, taking values after commas
            header = d_reader.fieldnames  # skip header of csv file
            for row in d_reader:
                if row['user_id'] == username and pwd == row['password']:
                    j = 1
                    break
            if j == 1:
                break
            else:
                print("user credentials didn't match")
    return username


def uinstruct():
    print("Please select one option:")
    print("1. New User Creation")
    print("2. Login")


def main():
    while 1:
        uinstruct()
        k = int(input())
        if k == 1:
            d = user_creation()
            if d:
                print("user have been created successfully")
                print("please login to access the files:")
                k1 = user_verify()
                if k1 is not None:
                    c_id = k1
                    break
        elif k == 2:
            k1 = user_verify()
            if k1 is not None:
                c_id = k1
                break
        else:
            print("invalid input")
    client_functions.menu()
    fv_map = {}
    while 1:
        c_input = sys.stdin.readline()
        if "<list>" in c_input:
            c_sock = client_functions.socket_connection()
            client_functions.directory(c_sock, "", "", "", 0, True)
            c_sock.close()
       # elif "<menu>" in c_input:
        #    client_functions.menu()
        elif "<create>" in c_input:
            while not client_functions.validity(c_input):  # error check the input
                c_input = sys.stdin.readline()
            flag = 0
            temp, filename, aType = c_input.split()
            resp = client_functions.handle_create(filename, aType, c_id, flag, fv_map)
            if resp:
                print("file has been created successfully")
        elif "<write>" in c_input:
            while not client_functions.validity(c_input):
                c_input = sys.stdin.readline()
            filename = c_input.split()[1]
            flag = 1
            resp = client_functions.handle_write(filename, c_id, flag, fv_map)
            if not resp:
                print("File unlock polling timeout")
            print("<write> mode exitted")
        elif "<read>" in c_input:
            while not client_functions.validity(c_input):
                c_input = sys.stdin.readline()
            filename = c_input.split()[1]
            client_functions.handle_read(filename, fv_map, c_id)
            print("<read> mode exitted")
        elif "<rename>" in c_input:
            while not client_functions.validity(c_input, 1):  # error check the input
                c_input = sys.stdin.readline()
            temp, oldfilename, newfilename = c_input.split()
            resp = client_functions.handle_rename(oldfilename, newfilename, c_id)
            if resp:
                print("file has been renamed successfully")
        elif "<delete>" in c_input:
            while not client_functions.validity(c_input):  # error check the input
                c_input = sys.stdin.readline()
            filename = c_input.split()[1]  # get file name from the input
            resp = client_functions.handle_delete(filename, c_id)  # handle the read request
            if resp:
                filename=filename+".txt"
                del fv_map[filename]
                print("Exiting <delete> mode...\n")
            else:
                print("Delete didn't perform successfully")
        elif "<quit>" in c_input:
            sys.exit()
        else:
            print("Selection is wrong")
            time.sleep(0.5)
            client_functions.menu()


if __name__ == "__main__":
    main()
