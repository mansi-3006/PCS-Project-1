# PCS-Project-1

We have created below mentioned modules:
  (1) 3 Servers
  (2) Directory Service
  (3) Locking Service
  
Our DFS system can attend 10000 parallel requests at a time. Every file will have total 3 copies of them. We have added permissions like read only, read write and restricted and based on the permissions on the file, clients can create, read, write, delete and rename the file.

In order to run the system, we need to start 3 servers, directory service and locking service using "python file.py". And then run the client on different machines.

We have implemented the locking service using which we can manage the file versions when multiple clients are accessing the same file. As an example if 2 clients are requesting to write in same file then the first request will be considered and system will lock that file until first request is completed. After completion of first client's request, our system will unlock the file and second client can access it. 

We have added the login service also. To store the usernames and passwords for login service, we are using logins.csv file. 

Our system contains name server and file server. Here, 3 severs are file server and directory service is name service. Directory service is used to keep track of the files and copies of files present in which server with the version of the files. To store the data of the files we are using filemappings.json file.



  <list> - List all existing files
  <create> [filename] [permission]- Create the file
  <write> [Filename] - Write text to a file
  <read> [Filename] - Read from a file
  <rename> [oldfilename] [newfilename] - Rename the file
  <delete> [filename] - Delete the file
  <quit> - Quit from the application
    
    
    
Members
Kaushik Allu Suresh(KA41131)
Mansi Patel(BV96417)
Nishi Ajmera(JU61134)
Sai Krishna(GU24496)
Shankar Sharma(QZ51136)
