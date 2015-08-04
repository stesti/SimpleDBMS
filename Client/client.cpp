//
//  ==========================================
//  ||  Simple DBMS System - Client         ||
//  ==========================================
//
//  Coded in 2009 by
//  Davide.Perina@studenti.unitn.it
//  Stefano.Testi@studenti.unitn.it
//
//  This work is released under the Creative Commons License
//  Attribution-Noncommercial-Share Alike 3.0
//
//

#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <stdio.h>
#include <netdb.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <arpa/inet.h>
#include <list>

using namespace std;



// DATA TYPES
//
// Declaring data types:
//     - replica
//           This data type is used to represent the coordinates to access
//           one of the replicated DB servers. It contains an integer identifier
//           for the replica, the IP address and the port.
//     - couple
//           This data type is used to represent the specific operation to perform on
//           one of the servers.
//     - db_msg
//           This is the message exchanged between the client and the servers,
//           and between the servers.


struct replica {
	int id;						// Server ID
	char ip[15];				// Server IP
	int port;					// Server Port
};


struct couple {
	char operation;				// Operation: Q, D or I
	int key;					// Key
	int value;					// Value
};

struct db_msg {
	struct couple cpl;			// Operation to perform and parameters
	char cs;					// Message involving a client (C) or inter-server (S)
	char cmd;					// Command: Commit (R)equest, (C)ommit Response, (A)bort, ...
	int opid;					// Operation identifier. On the Client->Server message it is -1
	int from;					// Msg coming from the server 'from'
	int to;						// Msg for the server 'to'
};



// FUNCTIONS


// print_error()
//
// This function prints on the screen the list of parameters to use
// when this program is executed

void print_error(){
	
	cerr << "\nERROR! You should specify the operation to perform on the DB!" << endl;
	cerr << "You should use either: " << endl;
	cerr << "Q k s   -> to perform a Query operation on the item associated to key k using server s, with k and s integer" << endl;
	cerr << "D k   -> to perform a Delete operation on the item associated to key k, with k integer" << endl;
	cerr << "I k v -> to perform an Insert operation with key k and value V, with k and v integers\n" << endl;
	
}



// list<replica> format_line(string FileLineID, string FileLineIP, string FileLinePort, list<replica> replicalist)
//
// The format_line function takes as input some parameters related to
// a single db servers read from a single line of the servers file, and
// appends them into the list of servers.

list<replica> format_line(string FileLineID, string FileLineIP, string FileLinePort, list<replica> replicalist){
	
	// Creating a temporary replica and filling it
	struct replica replica_temp;
	replica_temp.id = atoi(FileLineID.c_str());
	strcpy(replica_temp.ip,FileLineIP.c_str());
	replica_temp.port = atoi(FileLinePort.c_str());
	
	// Appending the new server to the list of db servers
	replicalist.push_back(replica_temp);
	
	// Returning the new list of servers
	return replicalist;

}



// list<replica> read_file(list<replica> replicalist)
//
// The read_file function reads the file containing the list of the
// db servers, and, line by line, passes these parameters to the
// format_line function, to append them into the list of servers.

list<replica> read_file(list<replica> replicalist){

	// Opening server list file
	ifstream file("serverlist.txt");

    string FileLineID;
	string FileLineIP;
	string FileLinePort;
	
    while (file) {
	
		// Withdrawing server parameters from a single line of the servers file
		file >> FileLineID >> FileLineIP >> FileLinePort;
		
		if ( !file ) {
			if ( !file.eof() ) {
				// Error handling
				perror ("Error! Stream failure!");
			}
		} else {
			// Passing the single line to the format_line function
			replicalist = format_line( FileLineID, FileLineIP, FileLinePort, replicalist);
		}
		
	}

	// Closing server list file
    file.close();

	// Returning the list of servers filled with the data from the servers file
	return replicalist;

}



// compare_repid(replica pos1, replica pos2)
//
// Compare function, to sort the list of servers by the ID server

bool compare_repid(replica r1, replica r2){
	if(r1.id < r2.id ){
		return true;
	} else {
		return false;
	}
	 
}



// perform_operation(char ip[], int port, char operation, int key, int value)
//
// The perform_operation connects to a specific server, sends the operation
// (cointained in the couple struct) and waits for the response (another couple
// struct) from the db server.

struct db_msg perform_operation(char ip[], int port, char operation, int key, int value, int srv) {

	int sockfd;
	
	// Creating the message to send to the server and to
	// fill with the response from the server

	struct db_msg messg;
	messg.cpl.operation = operation;
	messg.cpl.key = key;
	messg.cpl.value = value;
	messg.to = srv;
	messg.cs = 'C';
	messg.opid = -1;
	messg.cmd = 'Z';

	// Setting the connection parameters
	struct sockaddr_in server_addr;	
	server_addr.sin_family = AF_INET;
	server_addr.sin_addr.s_addr = inet_addr(ip);
	server_addr.sin_port = htons(port);
	
	// Creating the socket
	if ((sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP))<0) {
		// Error handling
		perror ("Error");
		exit(2);
	}

	// Trying to connect to the server
	int err = connect(sockfd, (struct sockaddr *) &server_addr, sizeof(server_addr));
	if(err != 0) {
		// Error handling
		perror ("Error");
		exit(3);		
	}

	// Sending the operation TO the server
	send (sockfd, (struct db_msg*) &messg, sizeof(db_msg), 0);

	// Waiting for the response FROM the server
	int len = recv(sockfd,(struct db_msg*) &messg, sizeof(db_msg), 0);
	
	// Closing socket
	close(sockfd);
	
	// Returning the response received from the server
	return messg;

}



// db_query(int key, int srv, list<replica> replicalist)
//
// This function performs the QUERY operation. It takes as input the key to query,
// the server on which to perform the operation, and the list of available servers.

void db_query(int key, int srv, list<replica> replicalist){
	
	struct replica server;
	bool find = 0;
	
	// Searching if the server to contact is inside the server list
	for (list<replica>::iterator it = replicalist.begin(); it!=replicalist.end(); it++){
		if ((*it).id == srv){
			server = (*it);
			find = 1;
			break;
		}
	}
	
	// Error handling
	if (find == 0) {
		cerr << "The server requested is not available\n";
		exit(4);
	}
	
	// Printing on the screen informations about the operation to perform
	cout << "---- You want to QUERY the key " << key << " from server " << srv << endl;
	cout << "---- The server to contact is " << server.id << " at IP " << server.ip << endl;

	// Sending the operation to perform_operation
	struct db_msg messg = perform_operation(server.ip, server.port, 'Q', key, 0, server.id);
	
	// The result of the operation, received from the server
	if ((messg.cmd) == 'K' ){
		cout << "OK - QUERY of key " << messg.cpl.key << " committed on server " << server.id << "!" << endl;
		cout << "     RESULT: key " << messg.cpl.key << " = " << messg.cpl.value << endl;
	} else {
		if ((messg.cmd) == 'A' ){
			cout << "OK - QUERY of key " << messg.cpl.key << " committed on server " << server.id << "!" << endl;
			cout << "     RESULT: key not present" << endl;
		} else {	
			if ((messg.cmd) == 'N' ){
				cout << "ABORT - QUERY of key " << messg.cpl.key << " has been aborted by the distributed DB!" << endl;
			} else {
				// Error handling
				cerr << "ERROR! Cannot understand the response from the DB server!" << endl;
			}
		}
	}

}



// db_delete(int key, list<replica> replicalist)
//
// This function performs the DELETE operation. It takes as input the key to delete,
// and the list of available servers.

void db_delete(int key, list<replica> replicalist){

	struct replica server;
	server.id = -1;
	
	// Searching for the nearest server to contact
	for (list<replica>::iterator it = replicalist.begin(); it!=replicalist.end(); it++){
		if ((*it).id <= key){
			server = (*it);
		} else {
			break;
		}
	}
	
	// Handling the situation for which there are no servers with
	// Server.ID <= key < Next Server.ID
	if (server.id == -1){
		cerr << "ERROR! No server is available to serve operations with this key!" << endl;
		exit(5);
	}
	
	// Printing on the screen informations about the operation to perform
	cout << "---- You want to DELETE key " << key << endl;
	cout << "---- The server to contact is " << server.id << " at IP " << server.ip << endl;

	// Sending the operation to perform_operation
	struct db_msg messg = perform_operation(server.ip, server.port, 'D', key, 0, server.id);
	
	// The result of the operation, received from the server
	if ((messg.cmd) == 'K' ){
		cout << "OK - DELETE of key " << messg.cpl.key << " committed on server " << server.id << "!" << endl;
	} else {
		if ((messg.cmd) == 'N' ){
			cout << "ABORT - DELETE of key " << messg.cpl.key << " has been aborted by the distributed DB!" << endl;
		} else {
			if ((messg.cmd) == 'W' ){
				cerr << "ERROR! You contacted the wrong DB server!" << endl;
			} else {
				// Error handling
				cerr << "ERROR! Cannot understand the response from the DB server!" << endl;
			}
		}
	}

}



// db_insert(int key, int value, list<replica> replicalist)
//
// This function performs the INSERT operation. It takes as input the key to insert,
// the value, and the list of available servers.

void db_insert(int key, int value, list<replica> replicalist){
	
	struct replica server;
	server.id = -1;
	
	// Searching for the nearest server to contact
	for (list<replica>::iterator it = replicalist.begin(); it!=replicalist.end(); it++){
		if ((*it).id <= key){
			server = (*it);
		} else {
			break;
		}
	}
	
	// Handling the situation for which there are no servers with
	// Server.ID <= key < Next Server.ID
	if (server.id == -1){
		cerr << "ERROR! No server is available to serve operations with this key!" << endl;
		exit(5);
	}
	
	// Printing on the screen informations about the operation to perform
	cout << "---- You want to INSERT the value " << value << " in key " << key << endl;
	cout << "---- The server to contact is " << server.id << " at IP " << server.ip << endl;

	// Sending the operation to perform_operation
	struct db_msg messg = perform_operation(server.ip, server.port, 'I', key, value, server.id);
	
	// The result of the operation, received from the server
	if ((messg.cmd) == 'K' ){
		cout << "OK - INSERT of key " << messg.cpl.key << " committed on server " << server.id << "!" << endl;
	} else {
		if ((messg.cmd) == 'N' ){
			cout << "ABORT - INSERT of key " << messg.cpl.key << " has been aborted by the distributed DB!" << endl;
		} else {
			if ((messg.cmd) == 'W' ){
				cerr << "ERROR! You contacted the wrong DB server!" << endl;
			} else {
				// Error handling
				cerr << "ERROR! Cannot understand the response from the DB server!" << endl;
			}
		}
	}

}



// create_replicalist()
//
// Creating the list of servers, to fill with the data about the available
// servers coming from the file.

list<replica> create_replicalist(){
	
	// Creating the list
	list<replica> replicalist;
	
	// Asking read_file to read the server file and put these informations
	// in the list
	replicalist = read_file(replicalist);
	
	// Sorting the list by the ID of the server
	replicalist.sort(compare_repid);
	
	// Returning the list of servers filled with the informations taken
	// from the file
	return replicalist;

}



// main(int ARGC, char* ARGV[])
//
// The main body

int main(int ARGC, char* ARGV[]){
	
	int key = 0;
	int value = 0;
	list<replica> replicalist;
	
	switch (ARGC){
		
		// Two command line parameters, possible operation: DELETE
		case 3:
			
			if (atoi(ARGV[2])!=0){
			// Trying to convert to integer the key parameter
				key = atoi(ARGV[2]);
			} else {
				if (*ARGV[2]=='0') {
					key = atoi(ARGV[2]);
				} else {
					// Error handling
					print_error();
					exit(1);
				}
			}
		
			switch (*ARGV[1]) {
				
				// DELETE operation
				case 'D':
					// Creating the list of servers and filling it
					replicalist = create_replicalist();
					// Performing the DELETE operation
				    db_delete(key, replicalist);
					break;
					
				default:
					// Error handling
					print_error();
					exit(1);
			}
			
			break;
			
		// Three command line parameters, possible operation: QUERY or INSERT
		case 4:
		
			//if (atoi(ARGV[2])!=0 && atoi(ARGV[3])!=0){
			if (atoi(ARGV[2])!=0) {
				if (atoi(ARGV[3])!=0) {	
					// Trying to convert to integer the key and value parameter
					key = atoi(ARGV[2]);
					value = atoi(ARGV[3]);
				} else {
					if (*ARGV[3]=='0') {
						key = atoi(ARGV[2]);
						value = atoi(ARGV[3]);
					} else {
						// Error handling
						print_error();
						exit(1);
					}
				}
			} else {
				if (*ARGV[2]=='0') {
					if (atoi(ARGV[3])!=0) {	
						// Trying to convert to integer the key and value parameter
						key = atoi(ARGV[2]);
						value = atoi(ARGV[3]);
					} else {
						if (*ARGV[3]=='0') {
							key = atoi(ARGV[2]);
							value = atoi(ARGV[3]);
						} else {
							// Error handling
							print_error();
							exit(1);
						}
					}
				} else {
					// Error handling
					print_error();
					exit(1);
				}
			}
		
			switch (*ARGV[1]) {
				
				// QUERY operation
				case 'Q':
					// Creating the list of servers and filling it
					replicalist = create_replicalist();
					// Performing the QUERY operation
					db_query(key, value, replicalist);
					break;
					
				// INSERT operation
				case 'I':
					// Creating the list of servers and filling it
					replicalist = create_replicalist();
					// Performing the INSERT operation
					db_insert(key, value, replicalist);
					break;
					
				default:
					// Error handling
					print_error();
					exit(1);
			}
			break;
			
		// Error handling
		default:
			print_error();
			exit(1);
					
	}
	
	cout << endl;
}