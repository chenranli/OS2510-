package mutualExclusion;

import java.net.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.*;

public class Server {
	//every server will only send connection to each server whose server ID is smaller.
	private int serverID;   
	private ServerSocket serverSocket;
	private LogicClock clock;
	private String localFile = "";
	//operation list
	private ArrayList<Operation> oprList= new ArrayList<>();
	
	//there is a primary server that will write to original file
	private boolean primary;  
	private ArrayList<ArrayList<Object>> clientList = new ArrayList<>();
	private ArrayList<ArrayList<Object>> serverList = new ArrayList<>();
	private final ExecutorService communicationProcessingPool = Executors.newFixedThreadPool(5);
	
	public Server(int serverID, ServerSocket serverSocket, boolean primary) {
		this.serverID = serverID;
		this.serverSocket = serverSocket;
		this.primary = primary;
		
	}
	
	public static void main(String[] args) {
		try {
			BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));	
			
			//read port number
			int portNum = Server.getPortNum();
			ServerSocket serverSocket = new ServerSocket(portNum);
			
			//read server ID
			int serverID = Server.getServerID();
			
			//test if this server is primary or not
			boolean primary = Server.isPrimary();

			//create a server
			Server server = new Server(serverID, serverSocket, primary);
			
			//read total number of servers
			int numOfServer = Server.getNumOfServer();
			
			//number of incoming connections this server will receive
			int numOfInCon = numOfServer - server.serverID - 1;
			
			// server receives connections from other servers
			while (numOfInCon > 0) {
				numOfInCon -= 1;
				int otherServerID = numOfInCon + server.serverID + 1;
				System.out.println("server " + server.serverID + " is waiting for server " + otherServerID + " to connet:");
				Socket conFromOtherServer = serverSocket.accept();
				ArrayList<Object> otherServer = new ArrayList<>();
				//save information from currently incoming server
				otherServer.add(otherServerID);
				String otherIPAddress = conFromOtherServer.getRemoteSocketAddress().toString().split(":")[0].split("/")[1];
				otherServer.add(otherIPAddress);
				otherServer.add(conFromOtherServer.getPort());
				//add outputstream and inputstream to serverlist
				otherServer.add(new BufferedReader(new InputStreamReader(conFromOtherServer.getInputStream())));
				otherServer.add(new PrintWriter(conFromOtherServer.getOutputStream(), true));
				System.out.println("server " + otherServerID + " binded");
				System.out.println("IPaddress : " + otherIPAddress + "| port number : " + conFromOtherServer.getPort());
				System.out.println();
				System.out.println();
			}
			
			//number of outgoing connections sent by this server
			int numOfOutCon = server.serverID;
			//server send connections to other server that has smaller server ID
			while (numOfOutCon > 0) {
				numOfOutCon -= 1;
				int otherServerID = numOfOutCon;
				System.out.println("Waiting to send connection to server " + otherServerID + " .");
				System.out.println("Please enter IP address and port number of server " + otherServerID + " : ");
				String[] otherServerInfo = stdIn.readLine().split(" ");
				ArrayList<Object> otherServer = new ArrayList<>();
				Socket conToOtherServer = new Socket(otherServerInfo[0], Integer.parseInt(otherServerInfo[1]));
				otherServer.add(otherServerID);
				// may need more info
				otherServer.add(otherServerInfo[0]);
				otherServer.add(otherServerInfo[1]);
				//add outputstream and inputstream to serverlist
				otherServer.add(new BufferedReader(new InputStreamReader(conToOtherServer.getInputStream())));
				otherServer.add(new PrintWriter(conToOtherServer.getOutputStream(), true));
				System.out.println("New socket IP address is : " + conToOtherServer.getLocalAddress().toString());
				System.out.println("New socket port number is : " + conToOtherServer.getLocalPort());
				System.out.println("send connection to server " + otherServerID);
				System.out.println();
				System.out.println();
			}
			
			System.out.println("We have bind every server with each other!");
			
			while (true) {
				server.addClientSocket();
				
				//add client task to thread pool, client task is to read from that client
				server.communicationProcessingPool.submit(server.new ClientTask());
				//perform WRITE/READ operation
				if (server.allowedToEnter()) {
					if (server.oprList.get(0).getOprType() == "WRITE") {
						server.localFile += server.oprList.get(0).getUpdate();
						if (primary) {
							//primary server will write update to original file
							server.writeToLocal();
						}
						//WRITE operation is called by current server
						if (server.oprList.get(0).getID() == server.serverID) {
							server.release();
						}
					}else {//READ
						server.oprList.get(0).getOutputStream().println(server.localFile);
						server.release();
					}
				}
				
				
				
				
				
				
				
			}
			
			 
			
		}catch (IOException e) {
			return;
		}

	}
	

	private class ClientTask implements Runnable{
	
		@Override 
		public void run() {
			//read request from client.
			//1. broadcast this request to all other servers
			
			
		}
	}
	
	private void addClientSocket() {
		try {
			//timeout for waiting for a new client to connect
			serverSocket.setSoTimeout(40);
			Socket newClientSocket = this.serverSocket.accept();
			ArrayList<Object> newClient = new ArrayList<>();
			this.clientList.add(newClient);
			newClient.add(new BufferedReader(new InputStreamReader(newClientSocket.getInputStream())));
			newClient.add(new PrintWriter(newClientSocket.getOutputStream(), true)) ;
		}catch (IOException e) {
			
		}
	}
	
	private void writeToLocal() {
		//
	}
	
	private void release() {
		//
	}
	
	private boolean allowedToEnter() {
		//see who has sent a message
		Set commServer =new HashSet();
		for (Operation operation : this.oprList) {
			commServer.add(operation.getID());
		}
		/*
		 * In two cases, allow to enter: (1)calling server's READ operation is at the
		 * head of the queue, and all other servers have sent a message; (2)a WRITE
		 * operation is at the head of the queue, then all server write the operation to
		 * its local copy.
		 */
		int numOfServer = this.serverList.size();
		if ((commServer.size() == numOfServer && this.oprList.get(0).getID() == this.serverID)
				|| (commServer.size() != 0 && this.oprList.get(0).getOprType() == "WRITE")) {
			return true;
		}
		return false;
	}
	
	private class Operation {
		//serverID
		//operationType
		//content
		//timeStamp
		//outputstream
		private final int serverID = 0;
		private final String operationType = null;
		private final String update = "";
		private final PrintWriter out = null;
		
		private int getID() {
			return this.serverID;
		}
		
		private String getOprType() {
			return this.operationType;
		}
		
		private String getUpdate() {
			return this.update;
		}
		
		private PrintWriter getOutputStream() {
			return this.out;
		}
	}
	
	//read valid integer as port number
	private static int getPortNum() {
		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String inputLine = "";
		System.out.println();
		System.out.println("Usage: enter integer port number:");
		try {
			while (Server.invalidPortNum((inputLine = stdIn.readLine()))) {
			System.out.println("Invalid input, please try again.");			
			}	
		}catch (IOException e) {
			System.err.println("stop when reading port number");
		}
		return Integer.parseInt(inputLine);
	}
	
	//read valid server ID
	private static int getServerID() {
		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String inputLine = "";
		System.out.println();
		System.out.println("Please enter server ID:");
		try {
			while (Server.invalidServerID((inputLine = stdIn.readLine()))) {
			System.out.println("Invalid input, please try again.");			
			}
		}catch (IOException e) {
			System.err.println("stop when reading server ID");
		}
		return Integer.parseInt(inputLine);
	}
	
	//read valid integer indicating primary or not
	private static boolean isPrimary() {
		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String inputLine;
		boolean primary = false;
		System.out.println();
		System.out.println("Is this primary server(please enter 1 or 2): 1.YES  2.NO");
		try {
			while (Server.invalidPrimary((inputLine = stdIn.readLine()))) {
				System.out.println("Invalid input, please try again.");
			}		
			if(Integer.parseInt(inputLine) == 1) {
				primary = true;
			}
		} catch (IOException e) {
			System.err.println("stop when testing primary or not");
		}
		return primary;
	}
	
	//read valid integer as total number of servers
	private static int getNumOfServer() {
		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String inputLine = "";
		System.out.println();
		System.out.println("How many other servers there are:");
		try {
			while (Server.invalidNumOfServer((inputLine = stdIn.readLine()))) {
				System.out.println("Invalid input, please try again.");
			}		
		} catch (IOException e) {
			System.err.println("stop when reading total number of servers");
		}
		return Integer.parseInt(inputLine);
	}

	
	private static boolean invalidPortNum(String portNum) {
		//test 
		return false;
	}
	
	private static boolean invalidServerID(String serverID) {
		//test
		return false;
	}
	
	private static boolean invalidPrimary(String primary) {
		//test 
		return false;
	}
	
	private static boolean invalidNumOfServer(String numOfServer) {
		//test
		return false;
	}
	
	
	public int getID() {
		return this.serverID;
	}
	
	public ServerSocket getSocket() {
		return this.serverSocket;
	}
	
	

	private class LogicClock {
		private int clock = 0;
		
		public int tick() {
			return 1;
		}
		
		public int getTime() {
			return clock;
		}
		
		public void setTime(int time) {
			this.clock = time;
		}
	}
}
