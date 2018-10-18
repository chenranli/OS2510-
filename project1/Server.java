package mutualExclusion;

import java.net.*;
import java.util.concurrent.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.*;



public class Server {
	//every server will only send connection to each server whose server ID is smaller.
	private int serverID;   
	private ServerSocket serverSocket;
	private LogicClock clock = new LogicClock();
	private String localFile = "0:0";
	//there is a primary server that will write to original file, all other servers will only write to their local copies
	private boolean primary; 	
	private int numOfServer;
	//synchronized operation list
	private List<Operation> oprList= Collections.synchronizedList(new ArrayList<>()); 
	//synchronized client list  
	private List<ArrayList<Object>> clientList = new ArrayList<>();
	//synchronized server list 
	private List<ArrayList<Object>> serverList = new ArrayList<>();
	private final ExecutorService communicationProcessingPool = Executors.newFixedThreadPool(10);
	private final Semaphore clockLock = new Semaphore(1);
	private int[] allowCounter;
	private final Semaphore allowCounterLock = new Semaphore(1);
	
	public Server(int serverID, ServerSocket serverSocket, boolean primary, int numOfServer) {
		this.serverID = serverID;
		this.serverSocket = serverSocket;
		this.primary = primary;
		this.numOfServer = numOfServer;
		this.allowCounter = new int[numOfServer];
		init(this.allowCounter);
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
			
			//read total number of servers
			int numOfServer = Server.getNumOfServer();
			
			//create a server
			Server server = new Server(serverID, serverSocket, primary, numOfServer);
			
			//number of incoming connections this server will receive
			int numOfInCon = numOfServer - server.serverID - 1;
			
			// server receives connections from other servers
			while (numOfInCon > 0) {
				numOfInCon -= 1;
				int otherServerID = numOfInCon + server.serverID + 1;
				System.out.println("server " + server.serverID + " is waiting for server " + otherServerID + " to connet:");
				Socket conFromOtherServer = serverSocket.accept();
				ArrayList<Object> otherServer = new ArrayList<>();
				server.serverList.add(otherServer);
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
				server.serverList.add(otherServer);
				otherServer.add(otherServerID);
				// may need more info
				otherServer.add(otherServerInfo[0]);
				otherServer.add(otherServerInfo[1]);
				//add outputstream and inputstream to serverlist
				otherServer.add(new BufferedReader(new InputStreamReader(conToOtherServer.getInputStream())));
				otherServer.add(new PrintWriter(conToOtherServer.getOutputStream(), true));
				System.out.println("length of server list : " + server.serverList.size());
				System.out.println("New socket IP address is : " + conToOtherServer.getLocalAddress().toString());
				System.out.println("New socket port number is : " + conToOtherServer.getLocalPort());
				System.out.println("have sent connection to server " + otherServerID);
				System.out.println();
				System.out.println();
			}
			System.out.println();
			System.out.println("We have bind server " + server.serverID + " with all other server!");
			System.out.println();
			//listening to other servers
			for (ArrayList<Object> s : server.serverList) {
				server.communicationProcessingPool.submit(server.new ServerHandler((BufferedReader)s.get(3), (PrintWriter)s.get(4)));
				System.out.println("Server " + server.serverID + " is listening to server " + s.get(0));
				System.out.println();
			}
			
			while (true) {
				if (server.addClientSocket()) {
					ArrayList<Object> newClient = server.clientList.get(server.clientList.size() - 1);	
					//add new client task to thread pool, client task is to read from that client
					server.communicationProcessingPool.submit(server.new ClientHandler(server.serverID, newClient));
				}
				
				//perform WRITE/READ operation
				if (server.allowedToEnter()) {
					/*
					 * "WRITE" when there is a "WRITE" at the head of operation list that is called
					 * by current server and that "WRITE" has not been done before.
					 */
					if (server.oprList.get(0).getOprType().equals("WRITE")) {
						if (server.oprList.get(0).checkStat() == false) {
							String[] localFileInfo = server.localFile.split(":");
							int newNum = Integer.parseInt(server.oprList.get(0).getUpdate());
							String globalTime = (Integer.parseInt(localFileInfo[0]) + 1) + "";
							String sum = (Integer.parseInt(localFileInfo[1]) + newNum) + "";
							server.localFile = globalTime + ":" + sum;
							server.oprList.get(0).changeStat();
							if (primary) {
								// primary server will write update to original file
								server.updateOriginalFile();
							}
							// WRITE operation is called by current server
							if (server.oprList.get(0).getID() == server.serverID) {
								System.out.println("server " + server.oprList.get(0).getID()
										+ " is WRITING! Press Enter to continue:");
								String inputLine;
								if ((inputLine = stdIn.readLine()) != null) {
									System.out.println("Continue working...");
								}
								server.oprList.get(0).outputToClient().println(server.localFile);
								server.oprList.remove(0);
								server.release();
								synchronized (server.oprList) {
									for (Operation operation : server.oprList) {
										System.out.println("operation ID : " + operation.getID()
												+ "     operation type : " + operation.getOprType()
												+ "     operation time : " + operation.getTime());
									}
								}
								System.out.println();
							}
						}
					}else {
						//READ
						synchronized(server.oprList) {
							Iterator<Operation> iterator = server.oprList.iterator();
							Operation operation; 
							while (iterator.hasNext()) {
								operation = iterator.next();
								/*
								 * There is no available "READ" operation before a "WRITE" operation. Available
								 * means that "READ" is called by current server.
								 */														
								if (operation.getOprType().equals("WRITE")) {
									break;
								}
								//Find a "READ" operation called by current server. Perform this "READ".
								if (operation.getOprType().equals("READ") && operation.getID() == server.serverID
										&& operation.isDone == false) {
									operation.outputToClient().println(server.localFile);
									server.release();
									operation.changeStat();
									iterator.remove();
									break;
								}
							}
							
						}
					}
					server.sortOprList();
				}
			}
		}catch (IOException e) {
			return;
		}
	}
	
	/*
	 * Read message from other servers
	 */
	private class ServerHandler implements Runnable {
		private final BufferedReader in;
		private final PrintWriter out;
		
		private ServerHandler(BufferedReader in, PrintWriter out) {
			this.in = in;
			this.out = out;
		}
		
		@Override 
		public void run() {
			try {
				String message;
				while ((message = in.readLine()) != null) {
					String[] messageInfo = message.split("~");
					int messageServerID = Integer.parseInt(messageInfo[0]);
					String messageOprType = messageInfo[1];
					String messageUpdate = messageInfo[2];
					int messageTime =  Integer.parseInt(messageInfo[3]);
					System.out.println("receive a message from server " + messageServerID + " : message type : "
							+ messageOprType + "     message update : " + messageUpdate + "     message time : "
							+ messageTime);
					clock.setTime(Math.max(clock.getTime(), messageTime));
					clock.tick();
					if (messageOprType.equals("WRITE") || messageOprType.equals("READ")) {
						//add "WRITE" or "READ" request to operation list.
						oprList.add(new Operation(messageServerID, messageOprType, messageUpdate, messageTime, null));
						clock.tick();
						out.println(serverID + "~" + "ALLOW" + "~NOTHING~" + clock.getTime());
						System.out.println("send ALLOW message to server " + serverID);
						System.out.println();
					}else if (messageOprType.equals("ALLOW")) {
						oprList.add(new Operation(messageServerID, messageOprType, messageUpdate, messageTime, null));
					}else {
						//messageOprType == "RELEASE"
						synchronized(oprList) {
							Iterator<Operation> iterator = oprList.iterator();
							Operation operation;
							while (iterator.hasNext()) {
								operation = iterator.next();
								// delete first "WRITE" or "READ" operation send by that server
								if (operation.getID() == messageServerID
										&& (operation.getOprType().equals("WRITE") || operation.getOprType().equals("READ"))) {
									iterator.remove();
									break;
								}
							}
						}
					}
					System.out.println();
					System.out.println("Current operation list : ");
					sortOprList();
					synchronized(oprList) {
						for (Operation operation : oprList) {
							System.out.println("operation ID : " + operation.getID() + "     operation type : "
									+ operation.getOprType() + "     operation time : " + operation.getTime());
						}
					}
					System.out.println();
				}
			}catch (IOException e) {
				
			}
		}
	}
		
	/*
	 * Read request from client, add new operation to operation list, and send
	 * request to each server.
	 */
	private class ClientHandler implements Runnable{
		private final int serverID;
		private final ArrayList<Object> newClient;
		
		private ClientHandler(int serverID, ArrayList<Object> newClient) {
			this.serverID = serverID;
			this.newClient = newClient;
		}

		@Override 
		public void run() {
			try {
				BufferedReader in = (BufferedReader)this.newClient.get(0);
				String request;
				System.out.println("We have found a new client!!!!!!");
				while ((request = in.readLine()) != null) {
					String[] oprInfo = request.split("~");
					String oprType = oprInfo[0];
					String update = oprInfo[1];
					System.out.println(request);
					tick();
					int timeStamp = clock.getTime();
					PrintWriter out = (PrintWriter) this.newClient.get(1);
					oprList.add(new Operation(this.serverID, oprType, update, timeStamp, out));
					System.out.println("We have add a new operation to operation list : " + this.serverID + " " + oprType);
					System.out.println();
					System.out.println("Current operation list : ");
					sortOprList();
					synchronized(oprList) {
						for (Operation operation : oprList) {
							System.out.println("operation ID : " + operation.getID() + "     operation type : "
									+ operation.getOprType() + "     operation time : " + operation.getTime());
						}
					}
					System.out.println();
					Iterator<ArrayList<Object>> iterator = serverList.iterator();
					ArrayList<Object> server;
					while (iterator.hasNext()) {
						server = iterator.next();
						PrintWriter receiver = (PrintWriter) server.get(4);
						receiver.println(serverID + "~" + oprType + "~" + update + "~" + timeStamp);
						System.out.println("send a message to server " + server.get(0) + " : " + serverID + "~"
								+ oprType + "~" + update + "~" + timeStamp);
						System.out.println();
					}
				}
			}catch (IOException e) {
			
			}
		}
	}
	
	/*
	 * After operation is done, first remove exact one "ALLOW" message sent from
	 * each other server. If it has not received "ALLOW" message from some server,
	 * then wait for that "ALLOW" message. Finally send release message to each
	 * other server.
	 * 
	 */
	private void release() {
		try {
			this.allowCounterLock.acquire();
			int removeAllow = this.numOfServer - 1;
			while (removeAllow >= 0) {
				this.allowCounter[removeAllow] += 1;
				removeAllow -= 1;
			}
			this.allowCounter[this.serverID] = 0;
		}catch (InterruptedException e) {
			
		}finally {
			this.allowCounterLock.release();
		}

		this.tick();
		PrintWriter receiver;
		for (ArrayList<Object> server : serverList) {
			receiver = (PrintWriter) server.get(4);
			System.out.println("send RELEASE message to server : " + server.get(0));
			System.out.println();
			receiver.println(this.serverID + "~RELEASE~NOINFO~" + this.getTime());
		}
		
		
	}

	//sort operation list based on the time stamp of operation
	private void sortOprList() {
		this.oprList.sort(new Comparator<Operation>() {
			@Override
			public int compare(Operation opr1, Operation opr2) {
				return opr1.getTime() - opr2.getTime();
			}
		});
		//remove ancient "ALLOW"
		try {
			this.allowCounterLock.acquire();
			synchronized (this.oprList) {
				Operation operation;
				Iterator<Operation> iterator = this.oprList.iterator();
				while (iterator.hasNext()) {
					operation = iterator.next();
					int operationID = operation.getID();
					if (operationID != this.serverID && operation.getOprType().equals("ALLOW")
							&& this.allowCounter[operationID] > 0) {
						this.allowCounter[operationID] -= 1;
						iterator.remove();
					}
				}
			}
		}catch (InterruptedException e) {
			
		}finally {
			this.allowCounterLock.release();
		}
	}
	
	private int getTime() {
		try {
			this.clockLock.acquire();
			return this.clock.getTime();
		}catch (InterruptedException e) {
			
		}finally {
			this.clockLock.release();
		}
		return 0;
	}
	
	private void tick() {
		try {
			this.clockLock.acquire();
			this.clock.tick();
		}catch (InterruptedException e) {
			
		}finally {
			this.clockLock.release();
		}
	}
	
	private void setTime(int time) {
		try {
			this.clockLock.acquire();
			this.clock.setTime(time);
		}catch (InterruptedException e) {
			
		}finally {
			this.clockLock.release();
		}
	}
	
	private boolean addClientSocket() {
		try {
			//timeout for waiting for a new client to connect
			serverSocket.setSoTimeout(40);
			Socket newClientSocket = this.serverSocket.accept();
			ArrayList<Object> newClient = new ArrayList<>();
			this.clientList.add(newClient);
			newClient.add(new BufferedReader(new InputStreamReader(newClientSocket.getInputStream())));
			newClient.add(new PrintWriter(newClientSocket.getOutputStream(), true));
			return true;
		}catch (IOException e) {
			return false;
		}
	}
	
	private void updateOriginalFile() {
		//
	}		

	/*
	 * In two cases, allows to enter: (1)calling server's "READ" operation is before
	 * the first "WRITE" operation, and current server has received at lease a
	 * message from all other servers; (2)a WRITE operation is at the head of the
	 * queue, and current server has received at least a message from all other
	 * servers, then all server write the operation to its local copy.
	 */
	private boolean allowedToEnter() {
		//see who has sent a message
		Set<Integer> commServer =new HashSet<>();
		synchronized (this.oprList) {
			for (Operation operation : this.oprList) {
				commServer.add(operation.getID());
			}
		}
		//Check if current server has received some message from all other servers. 
		if (commServer.size() == this.numOfServer) {
			if (this.oprList.get(0).getOprType().equals("WRITE")) {
				return true;
			}
			synchronized (this.oprList) {
				for (Operation operation : this.oprList) {
					if (operation.getOprType().equals("READ") && operation.getID() == this.serverID) {
						return true;
					}
					if (operation.getOprType().equals("WRITE")) {
						return false;
					}
				}
			}
		}
		return false;
	}
	
	private void init(int[] array) {
		int size = array.length;
		for (int i = 0; i < size; i ++) {
			array[i] = 0;
		}
	}
	
	private class Operation {
		private final int serverID;
		private final String oprType;
		private final String update;
		private final int timeStamp;
		private final PrintWriter out;
		//whether this operation has been performed
		private boolean isDone = false;
		
		private Operation(int serverID, String oprType, String update, int timeStamp, PrintWriter out) {
			this.serverID = serverID;
			this.oprType = oprType;
			this.update = update;
			this.timeStamp = timeStamp;
			this.out = out;
		}
		
		private int getID() {
			return this.serverID;
		}
		
		private String getOprType() {
			return this.oprType;
		}
		
		private String getUpdate() {
			return this.update;
		}
		
		private PrintWriter outputToClient() {
			return this.out;
		}
		
		private void changeStat() {
			this.isDone = true;
		}
		
		private boolean checkStat() {
			return this.isDone;
		}
		
		private int getTime() {
			return this.timeStamp;
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
		System.out.println("How many servers there are:");
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
			return clock += 1;
		}
		
		public int getTime() {
			return clock;
		}
		
		public void setTime(int time) {
			this.clock = time;
		}
	}
}
