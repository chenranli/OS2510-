package mutualExclusion;

import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.io.*;

public class Server {
	// every server will only send connection to each server whose server ID is
	// smaller.
	private int serverID;
	private ServerSocketChannel serverChannel;
	private LogicClock clock = new LogicClock();
	private String localFile = "0:0";
	// there is a primary server that will write to original file, all other servers
	// will only write to their local copies
	private boolean primary;
	private int numOfServer;
	private List<Operation> oprList = new ArrayList<>();
	private List<NewClient> clientList = new ArrayList<>();
	private List<OtherServer> serverList = new ArrayList<>();
	private List<Operation> releaseList = new ArrayList<>();

	public Server(int serverID, ServerSocketChannel serverChannel, boolean primary) {
		this.serverID = serverID;
		this.serverChannel = serverChannel;
		this.primary = primary;
		this.numOfServer = 3;
	}

	public static void main(String[] args) {
		try {
			BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));

			// read port number
			int portNum = Server.getPortNum();
			ServerSocketChannel serverChannel = ServerSocketChannel.open();
			serverChannel.bind(new InetSocketAddress(portNum));

			// read server ID
			int serverID = Server.getServerID();
			boolean primary = (serverID == 0);
			
			// create a server
			Server server = new Server(serverID, serverChannel, primary);

			// number of incoming connections this server will receive
			int numOfInCon = server.numOfServer - server.serverID - 1;

			// server receives connections from other servers
			while (numOfInCon > 0) {
				numOfInCon -= 1;
				int otherServerID = numOfInCon + server.serverID + 1;
				System.out.println(
						"server " + server.serverID + " is waiting for server " + otherServerID + " to connet:");
				System.out.println("You should go to server " + otherServerID + " to send connection.");
				SocketChannel fromOtherServer = serverChannel.accept();
				fromOtherServer.configureBlocking(false);
				server.serverList.add(new OtherServer(otherServerID, fromOtherServer));
				System.out.println("server " + otherServerID + " bound");
				System.out.println();
				System.out.println();
			}

			// number of outgoing connections sent by this server
			int numOfOutCon = server.serverID;
			// server send connections to other server that has smaller server ID
			while (numOfOutCon > 0) {
				numOfOutCon -= 1;
				int otherServerID = numOfOutCon;
				System.out.println("Waiting to send connection to server " + otherServerID + " .");
				System.out.println("Please enter IP address and port number of server " + otherServerID + " : ");
				String inputLine = stdIn.readLine();
				String[] otherServerInfo = inputLine.split(" ");
				SocketChannel toOtherServer = SocketChannel.open();
				toOtherServer.connect(new InetSocketAddress(otherServerInfo[0], Integer.parseInt(otherServerInfo[1])));
				toOtherServer.configureBlocking(false);
				server.serverList.add(new OtherServer(otherServerID, toOtherServer));
				System.out.println("have sent connection to server " + otherServerID);
				System.out.println();
			}
			System.out.println();
			System.out.println("We have bind server " + server.serverID + " with all other server!");
			System.out.println();
			
			serverChannel.configureBlocking(false);
			ByteBuffer buf = ByteBuffer.allocate(2048);
			
			while (true) {
				server.addClientChannel();
				// perform WRITE/READ operation
				if (server.allowedToEnter()) {
					/*
					 * "WRITE" when there is a "WRITE" at the head of operation list that is called
					 * by current server and that "WRITE" has not been done before.
					 */
					if (server.oprList.get(0).getOprType().equals("WRITE")) {
						if (server.oprList.get(0).checkStat() == false) {
							String[] localFileInfo = server.localFile.split(":");
							Thread.sleep(50);
							int newNum = Integer.parseInt(server.oprList.get(0).getUpdate());
							String globalTime = (Integer.parseInt(localFileInfo[0]) + 1) + "";
							server.localFile = globalTime;
							Thread.sleep(50);
							String sum = (Integer.parseInt(localFileInfo[1]) + newNum) + "";
							server.localFile += ":" + sum;
							System.out.println("------------------------------------------------------------------writing------------------------");
							System.out.println("local file : " + server.localFile);
							server.oprList.get(0).changeStat();
							if (primary) {
								// primary server will write update to original file
								String newFile = "sharedFile.txt";
								BufferedWriter writer = new BufferedWriter(new FileWriter(newFile, true));
								writer.append(globalTime + ":");
								writer.append(sum);
								writer.newLine();
								writer.close();
							}
							// WRITE operation is called by current server
							if (server.oprList.get(0).getID() == server.serverID) {
								server.release(server.oprList.get(0).getTime());
								server.oprList.remove(0);
							}
						}
					} else {
						// READ
						Iterator<Operation> iterator = server.oprList.iterator();
						Operation operation;
						boolean canRead = false;
						int index = 0;
						while (iterator.hasNext()) {
							operation = iterator.next();
							/*
							 * There is no available "READ" operation before a "WRITE" operation. Available
							 * means that "READ" is called by current server.
							 */
							if (operation.getOprType().equals("WRITE")) {
								break;
							}
							// Find a "READ" operation called by current server. Perform this "READ".
							if (operation.getOprType().equals("READ") && operation.getID() == server.serverID
									&& operation.checkStat() == false) {
								canRead = true;
								index = server.oprList.indexOf(operation);
								break;
							}
						}
						if (canRead == true) {
							System.out.println("------------------------------------------------------------------reading------------------------");
							buf.put((server.localFile + '-').getBytes()); // write to buffer
							buf.flip(); // read from buffer
							Operation read = server.oprList.get(index);
							read.toClient().write(buf);
							buf.clear();
							server.release(read.getTime());
							server.oprList.remove(read);
						}
					}
				}
				server.sortOprList();
				// read input from each client
				for (NewClient client : server.clientList) {
					ByteBuffer buf1 = ByteBuffer.allocate(1014);
					int bytesRead = client.getChannel().read(buf1); // write to buffer
					if (bytesRead != -1) {
						buf1.flip(); // read from buffer
						while (buf1.hasRemaining()) {
							client.addInput(buf1.get());
						}
					}
					server.parseClientInput(client.getInput(), client.getChannel());
					buf1.clear(); // write to buffer
				}

				// read input from each server
				for (OtherServer otherServer : server.serverList) {
					ByteBuffer buf2 = ByteBuffer.allocate(2048);
					int bytesRead = otherServer.getChannel().read(buf2);
					if (bytesRead != -1) {
						buf2.flip(); // read from buffer
						while (buf2.hasRemaining()) {
							otherServer.addInput(buf2.get());
						}
						buf2.clear();
					}
					server.parseServerInput(otherServer.getInput(), otherServer.getChannel());
				}
				server.sortOprList();
			}
		} catch (IOException e) {
			return;
		}catch (InterruptedException e) {
			
		}
	}


	private void parseServerInput(List<Byte> list, SocketChannel socketChannel) throws IOException {
		ByteBuffer sbuf = ByteBuffer.allocate(2048);
		while (list.contains((byte)'-')) {
			String opr = "";
			byte b;
			Iterator<Byte> iterator = list.iterator();
			while (iterator.hasNext()) {
				b = iterator.next();
				if (b == '-') {
					iterator.remove();
					break;
				}
				opr += (char) b;
				iterator.remove();
			}
			System.out.println("recieve a message : " + opr);
			String[] oprInfo = opr.split("~");
			this.tick();
			int timeStamp = Integer.parseInt(oprInfo[3]);
			this.setTime(Math.max(this.getTime(), timeStamp));
			if (oprInfo[1].equals("WRITE") || oprInfo[1].equals("READ")) {
				this.oprList.add(new Operation(Integer.parseInt(oprInfo[0]), oprInfo[1], oprInfo[2], timeStamp, socketChannel));
				System.out.println("add a new operation : " + oprInfo[0] + " " + oprInfo[1]);
				this.tick();
				System.out.println("send allow message : " + this.serverID + "~ALLOW~" + timeStamp + "~" + this.getTime() + "-");
				sbuf.put((this.serverID + "~ALLOW~" + timeStamp + "~" + this.getTime() + "-").getBytes());
				sbuf.flip();
				socketChannel.write(sbuf);
				sbuf.clear();
			} else if (oprInfo[1].equals("ALLOW")) {
				this.oprList.add(new Operation(Integer.parseInt(oprInfo[0]), "ALLOW", oprInfo[2], timeStamp, null));
			} else {
				// messageOprType == "RELEASE"
				this.releaseList.add(new Operation(Integer.parseInt(oprInfo[0]), "RELEASE", oprInfo[2], timeStamp, null));
			}
			this.sortOprList();
		}
	}

	// sort operation list based on the time stamp of operation
	private void sortOprList() {
		this.oprList.sort(new Comparator<Operation>() {
			@Override
			public int compare(Operation opr1, Operation opr2) {
				return opr1.getTime() - opr2.getTime();
			}
		});
		Iterator<Operation> releaseIterator = this.releaseList.iterator();
		while (releaseIterator.hasNext()) {
			Operation release = releaseIterator.next();
			Iterator<Operation> oprIterator = this.oprList.iterator();
			Operation opr;
			while (oprIterator.hasNext()) {
				opr = oprIterator.next();
				if (opr.getID() == release.getID() && opr.getTime() == Integer.parseInt(release.getUpdate())) {
					if (opr.getOprType().equals("READ")) {
						oprIterator.remove();
						releaseIterator.remove();
						break;
					} else if (opr.getOprType().equals("WRITE") && opr.checkStat() == true) {
						oprIterator.remove();
						releaseIterator.remove();
						break;
					}
				}
			}
		}
	}
	
	private boolean removeEnoughAllow(int[] allowCount) {
		for(int i : allowCount) {
			if (i ==1) {
				return false;
			}
		}
		return true;
	}
	
	private void parseClientInput(List<Byte> list, SocketChannel socketChannel) throws IOException {
		ByteBuffer cbuf = ByteBuffer.allocate(2048);
		if (list.contains((byte) '-')) {
			String opr = "";
			byte b;
			Iterator<Byte> iterator = list.iterator();
			while (iterator.hasNext()) {
				b = iterator.next();
				if (b == '-') {
					iterator.remove();
					break;
				}
				opr += (char) b;
				iterator.remove();
			}
			this.tick();
			int time = this.getTime();
			String[] oprInfo = opr.split("~");
			this.oprList.add(new Operation(this.serverID, oprInfo[0], oprInfo[1], time, socketChannel));
			if ( oprInfo[0].equals("READ")) {
				System.out.println("!!!!~~~~~~~~!!!!~~~~~~receive a read from client!!!!~~~!~!~!~!~!~!~!~!~!!~~~!~!~!~!~~~!~!~!~!~~~!~!~!~!~~~!~!~!~");
			}
			String test = this.serverID + "~" + oprInfo[0] + "~" + oprInfo[1] + "~" + time + "-";
			byte[] request = (this.serverID + "~" + oprInfo[0] + "~" + oprInfo[1] + "~" + time + "-").getBytes();
			for (OtherServer otherServer : this.serverList) {
				cbuf.put(request);
				cbuf.flip();
				otherServer.getChannel().write(cbuf);
				System.out.println("send request to server " + otherServer.getID() + " --- " + test);
				cbuf.clear();
			}
		}
		this.sortOprList();
	}

	private void addClientChannel() {
		try {
			SocketChannel clientChannel = this.serverChannel.accept();
			if (clientChannel != null) {
				clientChannel.configureBlocking(false);
				System.out.println("We have found a new client.");
				this.clientList.add(new NewClient(clientChannel));
			}
		} catch (IOException e) {

		}
	}

	/*
	 * After operation is done, first remove exact one "ALLOW" message sent from
	 * each other server. If it has not received "ALLOW" message from some server,
	 * then wait for that "ALLOW" message. Finally send release message to each
	 * other server.
	 * 
	 */
	private void release(int time) throws IOException {
		SocketChannel socketChannel;
		ByteBuffer buf = ByteBuffer.allocate(2048);
		this.tick();
		for (OtherServer otherServer : this.serverList) {
			socketChannel = otherServer.getChannel();
			//System.out.println("send RELEASE message to server : " + otherServer.getID());
			buf.put((this.serverID + "~RELEASE~" + time + "~" + this.getTime() + "-").getBytes());
			buf.flip();
			socketChannel.write(buf);
			buf.clear();
		}
		int[] allowCount = new int[this.numOfServer];
		for (int i = 0; i < this.numOfServer; i++) {
			allowCount[i] = 1;
		}
		allowCount[this.serverID] = 0;
		Operation operation;
		Iterator<Operation> iterator = this.oprList.iterator();
		while (iterator.hasNext()) {
			operation = iterator.next();
			int operationID = operation.getID();
			if (operation.getOprType().equals("ALLOW") && allowCount[operationID] > 0) {
				allowCount[operationID] -= 1;
				iterator.remove();
			}
		}
	}
	
	private int getTime() {
		return this.clock.getTime();
	}

	private void tick() {
		this.clock.tick();
	}

	private void setTime(int time) {
		this.clock.setTime(time);
	}

	/*
	 * In two cases, allows to enter: (1)calling server's "READ" operation is before
	 * the first "WRITE" operation, and current server has received at lease a
	 * message from all other servers; (2)a WRITE operation is at the head of the
	 * queue, and current server has received at least a message from all other
	 * servers, then all server write the operation to its local copy.
	 */
	private boolean allowedToEnter() {
		// see who has sent a message
		int counter = 0;
		if (this.oprList.size() > 0) {
			for (Operation operation : this.oprList) {
				if (operation.getOprType().equals("ALLOW") && Integer.parseInt(operation.getUpdate()) == this.oprList.get(0).getTime()) {
					counter += 1;
				}
			}
		}
		if (counter == this.numOfServer - 1) {
			return true;
		}
		if (oprList.size() > 0 && this.oprList.get(0).getOprType().equals("WRITE") && this.oprList.get(0).getID() != this.serverID) {
			return true;
		}
		//find doable READ operation
		int index = 0;
		for (Operation operation : this.oprList) {
			if (operation.getOprType().equals("READ") && operation.getID() == this.serverID) {
				index = this.oprList.indexOf(operation);
				break;
			}
			if (operation.getOprType().equals("WRITE")) {
				return false;
			}
		}
		counter = 0;
		if (index > 0) {
			for (Operation operation : this.oprList) {
				if (operation.getOprType().equals("ALLOW") && Integer.parseInt(operation.getUpdate()) == this.oprList.get(index).getTime()) {
					counter += 1;
				}
			}
			if (counter == this.numOfServer - 1) {
				return true;
			}
		}
		
		return false;
	}

	private class NewClient {
		private final SocketChannel clientSocketChannel;
		private final List<Byte> input = new ArrayList<>();

		private NewClient(SocketChannel clientChannel) {
			this.clientSocketChannel = clientChannel;
		}

		private SocketChannel getChannel() {
			return this.clientSocketChannel;
		}

		private List<Byte> getInput() {
			return this.input;
		}

		private List<Byte> addInput(byte b) {
			this.input.add(b);
			return this.input;
		}
	}

	// read valid integer as port number
	private static int getPortNum() {
		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String inputLine;
		System.out.println();
		System.out.println("Usage: enter integer port number:");
		try {
			inputLine = stdIn.readLine();
			return Integer.parseInt(inputLine);
		} catch (IOException e) {
			System.err.println("stop when reading port number");
		}
		return 0;
	}

	// read valid server ID
	private static int getServerID() {
		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String inputLine;
		System.out.println();
		System.out.println("Please enter server ID:");
		try {
			inputLine = stdIn.readLine();
			return Integer.parseInt(inputLine);
		} catch (IOException e) {
			System.err.println("stop when reading server ID");
		}
		return 0;
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
