package tinyGoogle;


import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.io.*;

public class Server {
	public static long startTime = 0;
	public static long endTime = 0;
	private static List<Map.Entry<String,Integer>> list = new ArrayList<>();
	private ServerSocketChannel serverChannel;
	//client who want to index some file
	private List<Queue> indexingList = new ArrayList<>();
	private List<SocketChannel> iHelperList = new ArrayList<>();
	private List<NewClient> clientList = new ArrayList<>();
	//index file list, should contain 27 items
	private List<String> indexFile = new ArrayList<>();
	private int masterPortNum = 10005;
	private final List<String> history = new ArrayList<>();
	private final ArrayList<SocketChannel> waitingList = new ArrayList<>();
	
	public Server(ServerSocketChannel serverChannel) {
		this.serverChannel = serverChannel;
	}

	public static void main(String[] args) {
		try {
			//0.ip 1.portForIndex 2.helpernumber 3.filepath 4.portForSearch 
			ServerSocketChannel serverChannel = ServerSocketChannel.open();
			InetSocketAddress inetSocketAddress = new InetSocketAddress(args[0], Integer.parseInt(args[1]));
			serverChannel.bind(inetSocketAddress);
			serverChannel.configureBlocking(false);
			Server server = new Server(serverChannel);
			int ihelpers = Integer.parseInt(args[2]);
			String filePath = args[3] + "\\";
			while (ihelpers > 0) {
				if (server.addIHelperChannel() == 1) {
					System.out.println("bind one helper");
					ihelpers -= 1;
				}
			}
			int searchPort = Integer.parseInt(args[4]);
			Thread searchThread = new Thread(new Runnable() {
				public void run() {
					server.start(searchPort);
				}			
			});		
			searchThread.start();
			server.init(filePath);
			while (true) {
				server.addSocketChannel();
				server.parseWaitingList();
				//check blockingqueue to see if first index-master has finished its job
				if (server.indexingList.size() > 0) {
					String feedback = server.indexingList.get(0).queue.peek();
					if (feedback != null) {
						endTime = System.nanoTime();
						System.out.println("total time for this task : " + ((endTime - startTime) / 1000000));
						startTime = 0;
						endTime = 0;
						System.out.println("finish index, pass success to client and remove corresponding item from indexingList");
						System.out.println();
						ByteBuffer buf = ByteBuffer.allocate(1024);
						buf.put("SUCCESS-".getBytes());
						buf.flip();
						server.indexingList.get(0).clientChannel.write(buf);
						buf.clear();
						server.indexingList.remove(0);
						server.updateFilePath();
					}
				}
				
				// read input from each client
				for (NewClient client : server.clientList) {
					ByteBuffer buf1 = ByteBuffer.allocate(1024);
					int bytesRead = client.getChannel().read(buf1); 
					if (bytesRead != -1) {
						buf1.flip();
						while (buf1.hasRemaining()) {
							client.addInput(buf1.get());
						}
					}
					server.parseClientInput(client.getInput(), client.getChannel());
					buf1.clear(); 
				}
				if (server.indexingList.size()>0 && server.indexingList.get(0).old == false) {
					//CREATE index master!!!!!!!!!!!!!!!!!!!
					server.indexingList.get(0).old = true;
					int masterPort = server.masterPortNum + 1;
					//System.out.println("~~~~~~~~~~~~port number for new master is : " + (server.masterPortNum + 1) + "~~~~~~~~~~~~~");
					Thread indexMaster = new Thread(new Runnable() {
						@Override
						public void run() {
							try {
								//create new file!!!!!!!!!!!!
								List<String> indexPathList = new ArrayList<>();
								int i = Integer.parseInt(String.valueOf(server.indexFile.get(0).charAt(server.indexFile.get(0).length() - 5)));
								i = (i + 1) %2;
								for (String filePath : server.indexFile) {
									BufferedReader r = new BufferedReader(new FileReader(filePath));
									String path = filePath.substring(0,  filePath.length() - 5) + i + ".txt";
									indexPathList.add(path);
									BufferedWriter w = new BufferedWriter(new FileWriter(path));
									String line;
									while ((line = r.readLine()) != null) {
										w.write(line);
										w.newLine();
									}
									r.close();
									w.close();
								}
								System.out.println("we are currently in master thread");
								List<Pair>  writingQueue = new ArrayList<>();
								ServerSocketChannel masterChannel = ServerSocketChannel.open();
								masterChannel.bind(new InetSocketAddress(masterPort));
								int helpers = server.iHelperList.size();
								List<Helper> helperList = new ArrayList<>(); 
								while (helpers > 0) {
									SocketChannel helperChannel= masterChannel.accept();
									if (helperChannel != null) {
										helperChannel.configureBlocking(false);
										helpers -= 1;
										System.out.println("master bind a new helper");
										helperList.add(server.new Helper(helperChannel));
									}
								}
								System.out.println("----finish binding helper-----");
								//pass filepath~startingline~endingline- to each helper, fileID is filePath
								BufferedReader reader = new BufferedReader(new FileReader(server.indexingList.get(0).filePath));
								int lines = 0;
								while ( reader.readLine() != null) {
									lines ++;
								}
								//System.out.println("there are # of lines in the file : " + lines + "-------------------");
								reader.close();
								int size = server.iHelperList.size();
								for (int helperIndex = 0; helperIndex < size; helperIndex ++) {
									ByteBuffer hbuf = ByteBuffer.allocate(1024);
									int startLine = (int)((float)lines / size * helperIndex) + 1;
									int endLine = (int)((float)lines / size * (helperIndex + 1));
									String s = server.indexingList.get(0).filePath + "~" + startLine + "~" + endLine + "-";
									System.out.println("---send index message to helper----");
									//System.out.println(s);
									System.out.println();
									hbuf.put(s.getBytes());
									hbuf.flip();
									helperList.get(helperIndex).helperChannel.write(hbuf);
									hbuf.clear();
								}
								//number of helper not finished writing
								int count = size;
								while (true) {
									for (Helper helper : helperList) {
										ByteBuffer helperBuf = ByteBuffer.allocate(1024);
										int bytesRead = helper.helperChannel.read(helperBuf); 
										if (bytesRead != -1) {
											helperBuf.flip();
											while (helperBuf.hasRemaining()) {
												helper.addInput(helperBuf.get());
											}
										}
									}
									//parse helper input
									for (Helper helper : helperList) {
										List<Byte> list = helper.input;
										if (list.contains((byte) '-')) {
											if (list.get(0) == (byte)'1') {
												list.remove(0);
												list.remove(0);
												writingQueue.add(server.new Pair(helper.helperChannel));
											}else {
												list.clear();
												count -= 1;
												writingQueue.remove(0);
											}
										}
									}
									if (writingQueue.size() != 0 && writingQueue.get(0).newTask == true) {
										writingQueue.get(0).newTask = false;
										ByteBuffer hbuf1 = ByteBuffer.allocate(4096);
										//allow~filepath~fp~fp~fp~fp...~fp-
										String message = "ALLOW~";
										for (String fp : indexPathList) {
											message += fp;
											message += "~";
										}
										message += "-";
										hbuf1.put(message.getBytes());
										hbuf1.flip();
										writingQueue.get(0).helperChannel.write(hbuf1);
										hbuf1.clear();
									}
									if (count == 0) {
										server.indexingList.get(0).queue.offer("S");
										return;
									}
								}
							} catch(IOException e) {
								
							}
							
						}
					});
					startTime = System.nanoTime();
					indexMaster.start();
					server.masterPortNum += 1;
					server.informHelper(args[0], server.masterPortNum);
					
				}
				
				
				
			}
		} catch (IOException e) {
		
		}
	}


	private void init(String filePath) throws IOException{
		char ch = 'A';
		File file;
		for (int i = 0; i < 26; i++) {
			this.indexFile.add(filePath + (char)((int)ch + i) + 0 + ".txt");
			file = new File(filePath + (char)((int)ch + i) + 0 + ".txt");
			file.createNewFile();
		}
		this.indexFile.add(filePath + "None0.txt");
		new File(filePath + "None0.txt").createNewFile();
	}
	
	private void parseClientInput(List<Byte> list, SocketChannel socketChannel) throws IOException {
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
			String[] oprInfo = opr.split("~");
			if ( oprInfo[0].equals("INDEX")) {
				System.out.println("!!!!~~~~~~~~!!!!~~~~~~receive an INDEX from client!!!!~~~!~!~!~!~!~!~!~!~!!~~~!~!~!~!~~~!~!~!~!~~~!~!~!~!~~~!~!~!~");
				System.out.println("----input file : " + oprInfo[0] + "----------");
				//create a index-master, pass to it : filepath,
				for (String file : this.history) {
					if (oprInfo[1].equals(file)) {
						ByteBuffer buf = ByteBuffer.allocate(1024);
						buf.put(("?").getBytes());
						buf.flip();
						socketChannel.write(buf);
						buf.clear();
						return;
					}
				}
				this.history.add(oprInfo[1]);
				BlockingQueue<String> queue = new ArrayBlockingQueue<>(1024);
				this.indexingList.add(new Queue(socketChannel, queue, oprInfo[1]));
			}
		}
	}

	
	private void addSocketChannel() {
		try {
			SocketChannel socketChannel = this.serverChannel.accept();
			if (socketChannel != null) {
				socketChannel.configureBlocking(false);
				this.waitingList.add(socketChannel);
				System.out.println("find a client or ihelper");
			}
		} catch (IOException e) {

		}
	}
	private void parseWaitingList() {
		try {
			ByteBuffer buf = ByteBuffer.allocate(1024);
			Iterator<SocketChannel> iterator = this.waitingList.iterator();
			while (iterator.hasNext()) {
				SocketChannel socketChannel = iterator.next();
				int bytesRead = socketChannel.read(buf);
				if (bytesRead != 0) {
					buf.flip();
					byte b = buf.get();
					if (b == (byte)'-') {
						iterator.remove();
						this.iHelperList.add(socketChannel);
						System.out.println("server bind a ihelper");
					}else {
						iterator.remove();
						NewClient newClient = new NewClient(socketChannel);
						this.clientList.add(newClient);
						System.out.println("We have found a new client.");
						newClient.addInput(b);
						while (buf.hasRemaining()) {
							newClient.addInput(buf.get());
						}
					}
				}
				buf.clear();
			}
		}catch (IOException e) {
			
		}
	}
	
	private int addIHelperChannel() {
		try {
			SocketChannel iHelperChannel = this.serverChannel.accept();
			if (iHelperChannel != null) {
				iHelperChannel.configureBlocking(false);
				System.out.println("A new index helper");
				this.iHelperList.add(iHelperChannel);
				return 1;
			}
			return 0;
		} catch (IOException e) {
			
		}
		return 0;
	}
	
	//update index file path
	private void updateFilePath() {
		int i = Integer.parseInt(String.valueOf((this.indexFile.get(0).charAt(this.indexFile.get(0).length() - 5))));
		i = (i + 1) %2;
		String filePath;
		for (int j = 0; j < 27; j ++) {
			filePath = this.indexFile.get(j);
			filePath = filePath.substring(0,  filePath.length() - 5) + i + ".txt";	
			this.indexFile.set(j, filePath);
		}
	}
	
	private void informHelper(String ipAddress, int port) throws IOException{
		for (SocketChannel helper : this.iHelperList) {
			ByteBuffer hbuf = ByteBuffer.allocate(1024);
			String s = ipAddress + "~" + port + "-";
			hbuf.put(s.getBytes());
			hbuf.flip();
			helper.write(hbuf);
			hbuf.clear(); 
		}
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

	private class Helper {
		private final SocketChannel helperChannel;
		private final List<Byte> input = new ArrayList<>();
		private Helper(SocketChannel helperChannel) {
			this.helperChannel = helperChannel;
		}
		private List<Byte> addInput(byte b) {
			this.input.add(b);
			return this.input;
		}
	}

	private class Pair {
		private final SocketChannel helperChannel;
		private boolean newTask = true;
		private Pair(SocketChannel helperChannel) {
			this.helperChannel = helperChannel;
		}
	}
	
	private class Queue {
		private final SocketChannel clientChannel;
		private final BlockingQueue<String> queue;
		private final String filePath;
		private boolean old;
		
		private Queue(SocketChannel clientChannel, BlockingQueue<String> queue, String filePath) {
			this.clientChannel = clientChannel;
			this.queue = queue;
			this.filePath = filePath;
			this.old = false;
		}

	}

	private static class ValueComparator implements Comparator<Map.Entry<String, Integer>>{
		public int compare(Map.Entry<String, Integer> m, Map.Entry<String, Integer> n) {
			return n.getValue()-m.getValue();
		}
	}

	public void start(int searchPort) {
		try 
		{
			ServerSocket serverSocket = new ServerSocket(searchPort);//begin a new Server
			Executor threadPool = Executors.newFixedThreadPool(100);//threadPool for masters 
			while(true) {
				Socket clientSocket = serverSocket.accept();//wait for a client connection
				threadPool.execute(new Runnable() {//take each new thread a new master to run helpers and return search result
					@Override
					public void run() {
						try {
							Socket conn = clientSocket;
							PrintWriter out = new PrintWriter(conn.getOutputStream(), true);
							BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
							String inputLine;
							while ((inputLine = in.readLine()) != null) {
								Server.list.clear();
								//HashSet<Integer> ans = new HashSet<Integer>();
								//HashSet<Integer> t = new HashSet<Integer>();
								BlockingQueue queue = new LinkedBlockingQueue();
								ExecutorService threadPool = Executors.newFixedThreadPool(10);
								threadPool.submit(new SearchHelper(1,inputLine,queue,indexFile));
								threadPool.submit(new SearchHelper(2,inputLine,queue,indexFile));
								threadPool.submit(new SearchHelper(3,inputLine,queue,indexFile));
								threadPool.submit(new SearchHelper(4,inputLine,queue,indexFile));
								threadPool.submit(new SearchHelper(5,inputLine,queue,indexFile));
								threadPool.submit(new SearchHelper(6,inputLine,queue,indexFile));
								threadPool.submit(new SearchHelper(7,inputLine,queue,indexFile));
								threadPool.submit(new SearchHelper(8,inputLine,queue,indexFile));
								threadPool.submit(new SearchHelper(9,inputLine,queue,indexFile));	
								try {
									threadPool.awaitTermination(1, TimeUnit.SECONDS);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								//System.out.println(queue);
								//System.out.println(ans);
								//out.println(ans);
								rank(queue);
								//out.println(tmp);
								out.println(list);
							}
						}catch(IOException e) {
							System.out.println(
									"Exception caught when trying to listen on port " + searchPort + " or listening for a connection");
							System.out.println(e.getMessage());
						}
						
					}
				});
			}	
		} catch (IOException e) {
			System.out.println(
					"Exception caught when trying to listen on port " + searchPort + " or listening for a connection");
			System.out.println(e.getMessage());
		}
	}
	
	public void rank(BlockingQueue q) {
		//for()
		LinkedHashMap<String,HashMap<String,Integer>> tmp = new LinkedHashMap<String,HashMap<String,Integer>>();
		while(!q.isEmpty()) {
			try {
				tmp.putAll((HashMap<String,HashMap<String,Integer>>)q.take());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//System.out.println(tmphash);
		System.out.println(tmp);
		LinkedHashMap<String,Integer> output = new LinkedHashMap<String,Integer>();
		for(HashMap<String,Integer> value: tmp.values()) {
			//System.out.println("value"+value);
			for(Map.Entry<String, Integer> entry: value.entrySet()) {
				if(output.containsKey(entry.getKey())) {
					//output.remove(entry.getKey());
					int a = entry.getValue()+output.get(entry.getKey());
					output.put(entry.getKey(),a);
				}else {
					output.put(entry.getKey(), entry.getValue());
				}
			}
		}
		//List<Map.Entry<Integer,Integer>> list = new ArrayList<>();
		list.addAll(output.entrySet());
		Server.ValueComparator vc = new ValueComparator();
		Collections.sort(list, vc);
	}

}