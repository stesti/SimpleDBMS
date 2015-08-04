//  ==========================================
//  ||  Simple DBMS System - Server         ||
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
#include <time.h>
#include <sys/wait.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <arpa/inet.h>

// Timeout in seconds for a pending operation to be declared 'in timeout'
#define lifetime_row 5

// How often to check the list of pending operations to clean it?
#define timeout 10

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
//     - pending
//           This is the data type used to represent a currently-ongoing operation.


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

struct pending {
	int opid;					// Operation identifier
	int status;					// Status of the operation: (1) if 2PC is initiated locally, (0) otherwise
	int pipe[2];				// Pipe sides to communicate with the child handling the 2PC
	int client_socket_fd;		// Socket FD to answer back to the client
	struct couple cpl;			// Operation to be performed
	char resp;					// Temporary value used during decision for Commit or Abort
	int pend;					// How many servers have still to respond?
	int pid_child;				// PID of the child handling the 2PC
	time_t optime;				// Time in which the operation entered the server
};

// GLOBAL VARIABLES
//
// Declaring global lists:
//     - replicalist, containing the list of servers belonging
//                    to the distributed DB
//     - pendinglist, containing a list of the operations which
//                    are currently being distributed

list<replica> *replicalist = new list<replica>;
list<pending> *pendinglist = new list<pending>;



// FUNCTIONS


// Compare function, to sort the list of servers by the ID server

bool compare_repid(replica r1, replica r2){
	if(r1.id < r2.id ){
		return true;
	} else {
		return false;
	}
	 
}



// void format_line(string FileLineID, string FileLineIP, string FileLinePort)
//
// The format_line function takes as input some parameters related to
// a single DB server read from a single line of the servers file, and
// appends them into the list of servers.

void format_line(string FileLineID, string FileLineIP, string FileLinePort){
	
	// Creating a temporary replica and filling it
	struct replica replica_temp;
	replica_temp.id = atoi(FileLineID.c_str());
	strcpy(replica_temp.ip,FileLineIP.c_str());
	replica_temp.port = atoi(FileLinePort.c_str());
	
	// Appending the new server to the list of db servers
	(*replicalist).push_back(replica_temp);

}



// int read_file(list<string> iplist)
//
// This function reads the list of servers from the file serverlist.txt
// and puts this data into a local list of servers.
// The list is filled only at the server start.
// This list won't be changed, until the server will be killed and the
// file newly read.

int read_file(list<string> iplist, int id){

	int port = -1;
	bool found = false; 
	
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
			format_line( FileLineID, FileLineIP, FileLinePort);
			
			//To find the port associated to ip of this machine
			if (found == false) {
				for (list<string>::iterator it = iplist.begin(); it!=iplist.end(); it++) {
					//cout << "(*it) è " << (*it) << endl;
					//cout << "FileLineIP è " << FileLineIP << endl;
					if (atoi(FileLineID.c_str())==id) {
						//cout << "HO TROVATO LA PORTA\n";
						found = true;
						
						port = atoi(FileLinePort.c_str());
					}
				}
			}
		}
	}
	
	// Closing server list file
    file.close();
	return port;

}




// void contact_host(struct db_msg messg, bool all)
//
// This function is called whenever a packet should be sent, both
// by the active or the passive side of the server.
// It takes as input the message to send, and a boolean value,
// used to specify if the message should be sent only to
// a specific server (all=false) or to all the servers
// (all=true).
// Everytime a new connection has to be set up, a new
// child process is spawned.

void contact_host(struct db_msg messg, bool all){
	
	pid_t pid;
	struct sockaddr_in addr;
	int sockfd;
	
	if ((messg.cs == 'C') || ((messg.cs == 'S') && (all == false))) {
		
		// If the message needs to be sent only to one server, a single
		// child is spawned.
		
		pid = fork();
		
		if (pid < 0) {
			
			// Error handling
			perror ("Error");
			
		} else {
			
			if (pid == 0) {
				
				// If the message needs to be sent to the CLIENT, the messg.from field
				// contains the identifier of the socket still open since the time
				// in which the client request has been received.
				// Only a send (and the socket closing) should be performed.
				
				if (messg.cs == 'C') {
					send(messg.from, (struct db_msg*) &messg, sizeof(db_msg),0);
					close(messg.from);
					exit(0);
					
				} else {
					
					// If the message needs to be sent to another server...
					
					for (list<replica>::iterator it = (*replicalist).begin(); it!=(*replicalist).end(); it++) {
						// Searching fot the IP and port of the recipient server...
						if (messg.to == (*it).id) {
							addr.sin_family = AF_INET;
							addr.sin_addr.s_addr = inet_addr((*it).ip);
							addr.sin_port = htons((*it).port);
							break;
						}
					}
					
					// Creating the socket
					if ((sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP))<0) {
						// Error handling
						perror ("Error");
						exit(2);
					}
					
					// Trying to connect to the server
					int err = connect(sockfd, (struct sockaddr *) &addr, sizeof(addr));
					if(err != 0) {
						// Error handling
						perror ("Error");
						close(sockfd);
						exit(3);		
					}

					// Sending the operation to the host
					send (sockfd, (struct db_msg*) &messg, sizeof(db_msg), 0);
					
					// Closing socket
					close(sockfd);
				}
				
				exit(0);
			}
		}
			
	} else {
		
		// If the message needs to be sent to all other servers (all=true), then a new
		// child is spawned for every server, the message sent, and then every child
		// will suicide. :)
		
		for (list<replica>::iterator it = (*replicalist).begin(); it!=(*replicalist).end(); it++) {
			
			// One fork for every server
			pid = fork();
			
			if (pid < 0) {
				
				perror ("Error");

			}else{
				
				if (pid == 0) {
					addr.sin_family = AF_INET;
					addr.sin_addr.s_addr = inet_addr((*it).ip);
					addr.sin_port = htons((*it).port);
					cout << "Into contact_host. messg.cs is " << messg.cs << " and all is " << all << ". Trying to connect to IP " << (*it).ip << " and port " << (*it).port << " messg.cmd is " << messg.cmd << " messg.cpl.operation is " << messg.cpl.operation << ", messg.cpl.key is " << messg.cpl.key << " and messg.cpl.value is " << messg.cpl.value << " and pidB is " << getpid() << endl;
					
					// Creating the socket
					if ((sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP))<0) {
						// Error handling
						perror ("Error");
						exit(2);
					}
					
					// Trying to connect to the host
					int err = connect(sockfd, (struct sockaddr *) &addr, sizeof(addr));
					if(err != 0) {
						// Error handling
						perror ("Error");
						close(sockfd);
						exit(3);		
					}
					
					messg.to = (*it).id;
					// Sending the operation to the host
					send (sockfd, (struct db_msg*) &messg, sizeof(db_msg), 0);
					
					// Closing socket
					close(sockfd);
					exit(0);
				}
			}
		}
	}
}



// struct couple perform_operation(struct couple record)
//
// This function starts the 2PC commit process, and then waits
// for the messages to be received by the father and written
// into the pipe.

struct couple perform_operation(struct couple record) {

	fstream file;
	string FileLineKey, FileLineValue;
	bool find = 0;
	list<couple> dblist;
	struct couple db_temp;
	
	cout << " -- Writing Operation " << record.operation << " on key " << record.key << " into Stable Storage..." << endl;
	
	// Is this (I)nsert, (Q)uery or (D)elete?
	switch (record.operation) {
		
		// INSERT operation
		case 'I':
			// Opening the file db.txt
			file.open("db.txt", ios::in);
			
			if (!file) {
				
				// If the file doesn't exist, it is created and the operation inserted
				// into the first row
				file.close();
				file.open ("db.txt", ios::out);
				file << record.key << " " << record.value;
				file.close();
			} else {
				
				// If the file exists, every row of the file is read, and put into a temporary
				// list of records (key,value), so the file is copied into the RAM
				while(!file.eof()){
					// A single row is read
					file >> FileLineKey >> FileLineValue;
					
					// Conversion
					db_temp.key = atoi(FileLineKey.c_str());
					db_temp.value = atoi(FileLineValue.c_str());
					dblist.push_back(db_temp);
					
					if (db_temp.key == record.key) {
						find = 1;
					}
				}
						
				if (find!=0) {
					file.close();
					
					// Overwriting the entire file db.txt
					file.open("db.txt", ios::out);
					
					for (list<couple>::iterator it = dblist.begin(); it!=dblist.end(); it++) {
						if ((*it).key != record.key){
							if (it==dblist.begin()) {
								file << (*it).key << " " << (*it).value;
							}else{
								file << endl << (*it).key << " " << (*it).value;
							}
						} else {
							// Here the modified row is written
							if (it==dblist.begin()) {
								file << record.key << " " << record.value;
							}else{
								file << endl << record.key << " " << record.value;
							}
						}
					}
					file.close();
					
				}else{
					
					// If an old entry for the key is not found in the file, the local copy is destroyed and
					// the new operation is appended at the end of the file.
					dblist.erase(dblist.begin(),dblist.end()); 
					file.close();
					
					// Opening the file in append mode (append the new row at the bottom of the file)
					file.open ("db.txt", ios::app | ios::out);
					
					// If the file is empty, the "endl" is not used
					if (FileLineKey=="") {
						file << record.key << " " << record.value;
					}else{
						file << endl << record.key << " " << record.value;
					}
					
					file.close();
				}
				
			}
			
			cout << " -- OK! Operation " << record.operation << " on key " << record.key << " written into Stable Storage!" << endl;
			
			// If the operation is succesfull, a message is sent back with a C in the field operation
			record.operation = 'C';
			break;

		// QUERY operation
		case 'Q':
			
			file.open ("db.txt", ios::in);
			
			while (file) {
				
				// Withdrawing server parameters from a single line of the servers file
				file >> FileLineKey >> FileLineValue;
				
				if ( !file ) {
					
					if ( !file.eof() ) {
						// Error handling
						perror ("Error! Stream failure!");
					}
					
				} else {
					
					if (atoi(FileLineKey.c_str()) == record.key) {
						find = 1;
						record.key = atoi(FileLineKey.c_str());
						record.value = atoi(FileLineValue.c_str());
						record.operation = 'C';
						break;
					}
					
				}
			}
			
			// If the key is not found...
			if (find==0) {
				cout << "-- WARNING! Query of key " << record.key << " not performed, since key " << record.key << " is not in the DB!" << endl;
				record.operation = 'A';
			} else {
				cout << " -- OK! Operation " << record.operation << " on key " << record.key << " performed!" << endl;
			}
			
			file.close();
			
			break;

		// DELETE operation
		case 'D':
		
			file.open("db.txt", ios::in);
			
			if (!file) {
				
				file.close();
				perror ("Error");
				//break;
				
			} else {
				
				// If the file exists, every row of the file is read, and put into a temporary
				// list of records (key,value), so the file is copied into the RAM.
				while(!file.eof()){
					file >> FileLineKey >> FileLineValue;
					
					if (atoi(FileLineKey.c_str()) == record.key) {
						find = 1;
					}else{
						db_temp.key = atoi(FileLineKey.c_str());
						db_temp.value = atoi(FileLineValue.c_str());
						dblist.push_back(db_temp);					
					}
				}
						
				if (find!=0) {
					// If the key has been found...
					file.close();
					
					// File is open in write mode and the temporary list written back to
					// the file
					file.open("db.txt", ios::out);
					for (list<couple>::iterator it = dblist.begin(); it!=dblist.end(); it++){
						if (it==dblist.begin()) {
							file << (*it).key << " " << (*it).value;
						}else{
							file << endl << (*it).key << " " << (*it).value;
						}
					}
					
					cout << " -- OK! Operation " << record.operation << " on key " << record.key << " written into Stable Storage!" << endl;
					
					file.close();
					
				}else{
					
					// If the key has not been found, everything is ok, since this is a Delete, and
					// the software behaves as if the key would have been found and successfully deleted.
					// In this case, only a Warning message is printed on the Server output.
					
					dblist.clear(); 
					cout << "-- WARNING! Delete of key " << record.key << " not performed, since key " << record.key << " is not in the DB!" << endl;
					file.close();
					
				}
				
			}
			
			record.operation = 'C';
			
			break;

	}

	return record;
}



// void agreement_start(struct db_msg messg)
//
// This function starts the 2PC commit process, and then waits
// for the messages to be received by the father and written
// into the pipe.

void agreement_start(struct db_msg messg){
	
	pid_t pid;
	int pipesides[2];
	struct pending agr;
	char resp;
	
	cout << " --- START of 2 phase commit for " << messg.cpl.operation << " on key " << messg.cpl.key << endl;
	
	// Assigning a random identifier for this operation
	messg.opid = random();
		
	// Creating pipes to communicate with father process and putting this operation
	// into the list of pending operations.
	pipe(pipesides);
	agr.opid = messg.opid;
	agr.cpl = messg.cpl;
	agr.pipe[0] = pipesides[0];
	agr.pipe[1] = pipesides[1];
	agr.resp = 'C';
	agr.pend = (*replicalist).size();
	agr.status = 1;
	agr.client_socket_fd = messg.from;
	agr.optime = time(0);
	
	(*pendinglist).push_back(agr);
	
	// Spawning a new child responsible for this specific operation
	messg.cs = 'S';
	messg.from = messg.to;
	messg.cmd = 'R';
	
	pid = fork();
	if (pid < 0) {
		
		perror ("Error");
		
	}else{
		
		if (pid == 0){
			
			close((*pendinglist).back().client_socket_fd);
			
			// The contact_host function will spawn 
			// many new children, one for every other server, and make them issue
			// a Commit Request (R) to each other server (including the passive side
			// of the current one)
			contact_host(messg, true);
			
			// Waiting for the Global Decision to be written into the pipe
			// by the father. This decision will be written once the father
			// has received all the response from all other servers.
			read(pipesides[0],&resp,1);

			// If the decision is a Commit (C), then the message to send to all other
			// server is a Global Commit (K), otherways it is a Global Abort (N)
			if (resp == 'C'){
				messg.cmd = 'K';
			} else {
				messg.cmd = 'N';
			}
			
			// The contact_host function will spawn 
			// many new children, one for every other server, and make them issue
			// the decision (K/N) to each other server (including the passive side
			// of the current one)
			contact_host(messg, true);
			
			// The pipe is closed and the current child killed
			close(pipesides[0]);
			close(pipesides[1]);
			exit(0);
			
		} else {
			
			(*pendinglist).back().pid_child = pid;		
				
		}
		
	}
	
}



// bool check_correct_server(struct db_msg messg)
//
// This function checks if this is the authoritative server
// for the received operation.

bool check_correct_server(struct db_msg messg){
	
	struct replica server;
	server.id = -1;
	
	for (list<replica>::iterator it = (*replicalist).begin(); it!=(*replicalist).end(); it++){
		if ((*it).id <= messg.cpl.key){
			server = (*it);
		} else {
			break;
		}
	}
	
	if (server.id != messg.to){
		return false;
	} else {
		return true;
	}
	
}



// bool find_active(int key, int opid)
//
// This function is used whenever we want to know if any operation
// on a specific key is already undergoing execution.
// If an operation on a specific key is received by the server from
// a client, and if another operation on the same key is currently
// being served by the Distributed DB, the newly received operation
// should be aborted.

bool find_active(int key, int opid) {

	bool found = false;
	
	for (list<pending>::iterator it = (*pendinglist).begin(); it!=(*pendinglist).end(); it++) {
		
		if ((*it).cpl.key==key) {
			
			// If the Pending List already contains an entry with the same key...
			
			if (opid==-1) {
				
				// If the message arrived is a Query operation and it is just 
				// arrived from a client, then another operation in the server
				// on the same key means that the new operation should be
				// aborted (return True).
				
				found = true;
				break;
				
			} else {
				
				if ((*it).opid!=opid) {
					
					// If the Operation is contained in the Pending List,
					// and the OpID between the two is different, this function
					// returns True.
					// The check on the OpID is performed to handle the situation
					// in which the active server sends the Commit Request to
					// himself, too. The Pending List already contains an item,
					// pushed back by the active part. This entry is related to
					// the same operation, so it's not related to another
					// operation, even if it is already in the Pending List.
					// In this case it returns False, in all the other cases,
					// True.
					
					found = true;
					break;
					
				}
				
			}
			
		}
		
		if ((*pendinglist).size()==0) break;
	}
	
	return found;
}



// handle_client(struct db_msg messg)
//
// This function is used if the packet received by the server
// is coming from the client.
// Based on the operation the client wants to perform,
// different actions are taken.

void handle_client(struct db_msg messg){

	struct couple temp_record;
	
	if ((messg.cpl.operation == 'Q') || (messg.cpl.operation == 'I') || (messg.cpl.operation == 'D')) {
		
		// If this is a QUERY...
		if (messg.cpl.operation == 'Q') {
			
			// Check if another operation is taking place on the same key
			// (otherways, Abort!)
			if(!find_active(messg.cpl.key,messg.opid)) {
				
				// Performing the read in the database
				temp_record = perform_operation(messg.cpl);
				
				// Packing the result, if the key is found
				if (temp_record.operation=='C') {
					messg.cpl = temp_record;
					messg.cmd = 'K';
					
				} else {
					// If the key is not found, an 'ABSENT' message is sent
					// back to the client
					if (temp_record.operation=='A') {
						messg.cmd = 'A';
					} else {
						// Error handling
						cout << "ERROR! The operation is not successful\n"; 
						messg.cmd = 'N';
					}
				}
				
			// Error handling
			} else {
				cerr << "ERROR! Another operation is taking place on this key!" << endl;
				messg.cmd = 'N';				
			}
			
		} else {
			
			// If this is an INSERT or a DELETE, check if the current is the correct server
			if (check_correct_server(messg)) {
				
				// Check if another operation is taking place on the same key
				// (otherways, Abort!)
				if(!find_active(messg.cpl.key,messg.opid)) {
					
					// Start 2 phase commit
					agreement_start(messg);
					return;
					
				} else {
					// Key is going to be updated
					cerr << "ERROR! Another operation is taking place on this key!" << endl;
					messg.cmd = 'N';

				}
			} else {
				
				// The client contacted the Wrong (W) server!
				messg.cmd = 'W';
				
			}
			
		}
		
	} else {
		// The command is neither Q, D or I
		messg.cmd = 'N';
	}
	
	// Sending back the reponse to the client
	contact_host(messg, false);
	close(messg.from);
	
}



// bool find_operation(int opid)
//
// This function is used whenever we want to know if a specified Operation
// is contained in the pending list.
// The search is performed by looking at the Operation identifier opid.

bool find_operation(int opid) {

	bool found = false;
	
	for (list<pending>::iterator it = (*pendinglist).begin(); it!=(*pendinglist).end(); it++) {
		
		if ((*it).opid==opid) {
			// If the specified operation is found, it returns True,
			// otherway it returns False.
			found = true;
			break;
		}
		
		if ((*pendinglist).size()==0) break;
		
	}
	
	return found;
	
}



// void handle_commit_request(struct db_msg messg)
//
// This function is called on the passive side whenever a Commit Request
// is received from an active server. The caller function, before
// calling handle_commit_request, first performs a check if another
// operation is currently being executed on the same key.

void handle_commit_request(struct db_msg messg) {

	struct pending agr;
	int temp_addr = messg.from;

	// The message to send back to the active server
	// is prepared
	messg.from = messg.to;
	messg.to = temp_addr;

	if ((messg.cmd=='C') && (!find_operation(messg.opid))) {
		// If no other operation is currently in progress on the same
		// key, the operation is pushed back into the local Pending
		// List
		agr.opid = messg.opid;
		agr.cpl = messg.cpl;
		agr.resp = messg.cmd;
		agr.status = 0;
		agr.client_socket_fd = -1;
		agr.pid_child = -1;
		agr.optime = time(0);
	
		(*pendinglist).push_back(agr);
	}
	
	// The response is sent back to the originating server
	// regardless to the fact that it could be a Commit or
	// an Abort.
	contact_host(messg, false);

}



// void handle_commit_response(struct db_msg messg)
//
// This function is called on the active side once a response to
// a Commit Request is received. If any of the server answers with
// an 'Abort', then the Global decision for that operation will
// be a Global Abort.

void handle_commit_response(struct db_msg messg) {

	list<pending>::iterator it;
	
	// Searching for the operation into the Pending List...
	for (it = (*pendinglist).begin(); it!=(*pendinglist).end(); it++) {
		
		if (((*it).opid==messg.opid) && ((*it).cpl.key==messg.cpl.key)) {
			// If the operation has been found...
		
			if (messg.cmd=='A') {
				// If any of the server answers with an Abort, the
				// Global Decision Variable is changed from the default
				// value (Commit) to an Abort.
				(*it).resp = 'A';
			}
			
			(*it).pend--;
			cout << "(*it).pend is " << (*it).pend << endl;
			break;
		}
		
		if ((*pendinglist).size()==0) break;
		
	}
	
	if (((*it).pend)==0) {
		
		// If all the passive servers have replied to the Commit Request,
		// the global decision is written into the Pipe connecting the
		// main process to the child which is responsible for the operation
		write((*it).pipe[1], &(*it).resp, 1);
		close((*it).pipe[1]);
		close((*it).pipe[0]);
		
		// The Pending field is clean to become an ACK counter
		(*it).pend = (*replicalist).size();
	}
}



// void handle_global_recv(struct db_msg messg)
//
// This function is called on the passive server side
// everytime a Global Decision (Global Commit or Global
// Abort) is received from an active server.

void handle_global_recv(struct db_msg messg) {

	struct couple temp_record;
	
	if (messg.cmd == 'K') {
		// If the received Decision is a Global Commit, the operation
		// is performed locally, and then the ACK is set as 'D'
		temp_record = perform_operation(messg.cpl);
		if (temp_record.operation=='C') {
			messg.cmd = 'D';
		} else {
			// If something goes wrong while performing the operation
			// locally, an Abort ACK is sent back to the active server
			cout << "--- AAARGH! Distributed DB lost synchrony! A PASV server had" << endl;
			cout << "            an error while writing an operation on the stable storage!" << endl;
			messg.cmd = 'E';
		}
	} else {
		// If the received Decision is a Global Abort, the ACK
		// is set as 'D'
		messg.cmd = 'D';
	}

	// The ACK is then sent back to the active server which sent the
	// Global Decision

	int temp_addr = messg.from;
	messg.from = messg.to;
	messg.to = temp_addr;
	contact_host(messg, false);
	
	// The operation has been either performed or aborted, so no additional
	// work should be done on the passive side, then the operation is removed
	// from the local passive Pending List
	for (list<pending>::iterator it = (*pendinglist).begin(); it!=(*pendinglist).end(); it++) {
		if (((*it).opid==messg.opid) && ((*it).cpl.key==messg.cpl.key) && ((*it).status==0)) {
			cout << "- END of operation " << messg.opid << ": locally " << messg.cmd << endl;
			(*pendinglist).erase(it);
			break;
		}
		if ((*pendinglist).size()==0) break;
	}
}



// void handle_ack(struct db_msg messg)
//
// This function is called on the active server side everytime
// an ACK from a passive server is received, related to the fact
// that the Global Decision has been received and the operation
// has been performed (in the case of Global Commit) or aborted (if
// the global decision was a Global Abort).

void handle_ack(struct db_msg messg) {

	list<pending>::iterator it;

	// A remote passive server has ended its work, so the active server
	// should wait for one less server to complete the work.

	for (it = (*pendinglist).begin(); it!=(*pendinglist).end(); it++) {
		if ((*it).opid==messg.opid && ((*it).cpl.key==messg.cpl.key)) {
			(*it).pend--;
			break;
		}
		if ((*pendinglist).size()==0) break;
	}
	
	// If the current ACK is the last acknowledgment (no more server
	// have to respond), the response to send back to the client
	// is prepared.
	
	if (((*it).pend)==0) {
		
		if ((*it).resp == 'C'){
			messg.cmd = 'K';
		} else {
			messg.cmd = 'N';
		}
		
		// Contacting the client...
		messg.cpl = (*it).cpl;
		messg.cs = 'C';
		messg.from = (*it).client_socket_fd;
		contact_host(messg,false);
		close(messg.from);
		
		// The 2PC is ended, and the operation is removed from the
		// list of pending operations.
		(*pendinglist).erase(it);		
	}
}



// void handle_server(struct db_msg messg)
//
// This function is called everytime a server receives
// a packet from another server. Based on the content
// of the message, a specific handler is called.

void handle_server(struct db_msg messg){
	
	if (messg.cmd == 'R'){
		
		// The received message is a COMMIT REQUEST. Hence, a check about
		// a possible ongoing operation on the same key is performed, 
		if(find_active(messg.cpl.key,messg.opid)) {
			cerr << "ERROR! Another operation is taking place on this key!" << endl;
			messg.cmd = 'A';
		} else {
			messg.cmd = 'C';
		}
		
		// Handling the COMMIT REQUEST received from the active server
		handle_commit_request(messg);
		
	} else {
		
		if (messg.cmd == 'C' || messg.cmd == 'A') {
			// Handling the COMMIT RESPONSE (COMMIT or ABORT) received from another
			// passive server
			handle_commit_response(messg);
			
		} else {
			
			if (messg.cmd == 'K' || messg.cmd == 'N') {
				// Handling the GLOBAL DECISION (GLOBAL COMMIT or GLOBAL ABORT) received
				// from the active server
				handle_global_recv(messg);
				
			} else {
				
				if (messg.cmd == 'D' || messg.cmd == 'E') {
					// Handling the ACK (D is ACK to Global Commit, E is ACK to Global Abort)
					// received from another passive server
					handle_ack(messg);
					
				} else {
					// Error Handling
					cerr << "ERROR! This server can't understand interserver response from another server!" << endl;	
									
				}
				
			}
			
		}
		
	}	
	
}



// void wait_for_packet(int port)
//
// This function waits for a new packet to arrive. The incoming packet
// could come from a client or another server. Based on the content
// on the packet (specifically, on the CMD field), the correct function
// (handle_server or handle_client) is called.

void wait_for_packet(int port){
	
	pid_t pid;
	struct sockaddr_in my_addr;
	//struct couple record;
	struct db_msg messg;
	int sockfd;
	
	// Creating the socket server side
	if ((sockfd = socket (AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
		perror ("Error");
		exit(1);
	}
	
	// Setting the server parameters
	my_addr.sin_family = AF_INET;
	my_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	my_addr.sin_port = htons(port);

	// Binding the socket with server address
	cout << "Starting server..." << endl;
	cout << "Binding..." << endl;
	if (bind(sockfd, (struct sockaddr *) &my_addr, sizeof(my_addr)) < 0) {
		perror ("Error");
		close(sockfd);
		exit(2);
	}
	cout << "Server started!" << endl;
	
	// The server is listening to incoming connections
	if (listen(sockfd, 10) < 0) {
		perror ("Error");
		close(sockfd);
		exit(3);
	}

	// Loop to serve all incoming connections
	while(1) {
		
		struct sockaddr_in addr;
		socklen_t len = sizeof(addr);
		
		// The server accepts the connections
		int conn_fd = accept(sockfd, (struct sockaddr*) &addr, &len);
		if (conn_fd < 0) {
			perror ("Error");
		}
		
		// The server receives the data from client
		int host_len = recv(conn_fd, (struct db_msg*) &messg, sizeof(db_msg),0);
		if (host_len < 0) {
			perror ("Error");
		}
		
		// Printing some informations on the screen... 
		cout << "> PACKET! From IP " << inet_ntoa(addr.sin_addr) << " port " << ntohs (addr.sin_port) << ": " << messg.cmd << " with " << messg.cpl.operation << " " << messg.cpl.key << " " << messg.cpl.value << endl;
		
		if (messg.cs == 'C'){
			
			// If the packet is coming from the client...
			messg.from = conn_fd;
			handle_client(messg);
			
		} else {
			
			if (messg.cs == 'S'){
				
				// If the packet is coming from another server...
				handle_server(messg);
				close(conn_fd);
				
			} else {
				
				// Closing socket
				close(conn_fd);
				cerr << "ERROR! This server can't understand the received MSG!" << endl;
				
			}
		}	
	}
}



// void handle_alarm (int signum)
//
// This function is called every time an alarm is generated. This
// happens every lifetime_row for all the servers, to remove
// dead undone operations and to notify the client with an
// abort.
// Timeout values are defined as constant at the beginning
// of this file (lifetime_row).

void handle_alarm (int signum) {

	struct db_msg messg;
	
	cout << "-> Check for undone operations in timeout..." << endl;
	
	for (list<pending>::iterator it = (*pendinglist).begin(); it!=(*pendinglist).end(); it++) {
		
		// Actual time
		time_t now = time(0);
		
		// If the current row is in timeout...
		if ((now >= (*it).optime+lifetime_row)) {
			
			cout << "   -> Operation timeout! Removing " << (*it).cpl.operation << " of key " << (*it).cpl.key << endl;
			
			// If this is the active server for the operation the client is informed...
			if ((*it).status==1) {
									
				cout << "   -> Notifying client about removal of " << (*it).cpl.operation << " of key " << (*it).cpl.key << endl;
				
				messg.cpl = (*it).cpl;
				messg.from = (*it).client_socket_fd;
				messg.cs = 'C';
				messg.cmd = 'N';
				
				// Notifying the client about the removal...
				contact_host(messg,false);				
				close(messg.from);
				
				cout << "   -> Terminating the child for " << (*it).cpl.operation << " of key " << (*it).cpl.key << endl;
				kill((*it).pid_child,SIGKILL);
				
			}
			
			// Purging the operation from the list
			(*pendinglist).erase(it);
			if ((*pendinglist).size()== 0) {
				break;
			} else {
				it--;
			}
			
			cout << "   -> Removal of dead operation done!" << endl;				
		}
	}
}



// char *getifaddr (char *ifname)
//
// This function reads a single network interface IP from the
// Operating System, and gives that as an output string.

char *getifaddr (char *ifname) {
    int fd;
    struct ifreq ifr;
    char *addr = NULL;
    struct sockaddr_in *sin = (struct sockaddr_in *) &ifr.ifr_ifru.ifru_addr;

    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
        return NULL;

    memset (&ifr, 0, sizeof (ifr));

    strcpy (ifr.ifr_name, ifname);
    sin->sin_family = AF_INET;

    if (ioctl (fd, SIOCGIFADDR, &ifr) == 0)
        addr = inet_ntoa (sin->sin_addr);

    close (fd);

    return addr;
}



// list<string> create_list_interface()
//
// This function creates the list of network interfaces
// based on data read from getifaddr function.

list<string> create_list_interface() {
	
	struct if_nameindex *ifni;
	list<string> iplist;
	char* ip;
	
	ifni = if_nameindex();
	
	while (ifni->if_name != NULL) {
		
		// For every interface...
        ip = getifaddr (ifni->if_name);

        if (ip != NULL) {
			iplist.push_back(ip);
		}
        
		ifni++;
    }
	
	return iplist;
	
}



// main()
//
// The main body

int main(int argc, char *argv[]) {
	
	// Creating the timer to destroy appended operations
	struct itimerval timer;
	list<string> iplist;
	int port;

	// To close all children and avoid zombies
	signal(SIGCHLD, SIG_IGN);

	// Signal to handle all processes blocked because an
	// active server is blocked
	signal(SIGALRM, handle_alarm);
	
	// Parameters of the timer, to look in the pending operation list
	// and decide whether or not some operation is in timeout
	timer.it_value.tv_usec = 0;
	timer.it_value.tv_sec = timeout;
	timer.it_interval.tv_usec = 0; 
	timer.it_interval.tv_sec = timeout;
	setitimer(ITIMER_REAL, &timer, NULL);
	
	// Initialization of random number generator (later used to assing random
	// identifier to every operation)
	srand (time(0));
	
	// Creation of network interfaces list of the server machine
	iplist = create_list_interface();
	
	// Creating the list of servers and filling it
	port = read_file(iplist, atoi(argv[1]));
	
	if(port==-1) {
		cout << "ERROR! In file serverlist.txt there are not the IP address and port for this machine\n";
		exit(0);
	} 
	
	// Sorting the list of servers by their ID
	(*replicalist).sort(compare_repid);

	// Going into wait mode for a new packet to arrive
	wait_for_packet(port);
    
}
