package mutualExclusion;

import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.*;

public class Client1{
	
	public static void main(String[] args) throws IOException{
		String fileName = args[0];
		
		ExecutorService threadPool = Executors.newFixedThreadPool(9);
		threadPool.submit(new Client1().new ClientTask(9999, true, 0, fileName));
		threadPool.submit(new Client1().new ClientTask(10001, true, 1, fileName));
		threadPool.submit(new Client1().new ClientTask(10001, true, 2, fileName));
		threadPool.submit(new Client1().new ClientTask(10001, false, 3, fileName));
		threadPool.submit(new Client1().new ClientTask(10000, false, 4, fileName));
		threadPool.submit(new Client1().new ClientTask(9999, false, 5, fileName));
	}
	
	
	private class ClientTask implements Runnable{
		private final int portNum;
		private final boolean readOrWrite;
		private final int clientID;
		private final String fileName;
		
		private ClientTask(int portNum, boolean readOrWrite, int clientID, String fileName) {
			this.portNum = portNum;
			this.readOrWrite = readOrWrite;
			this.clientID = clientID;
			this.fileName = fileName;
		}
		@Override
		public void run() {
			try {
				SocketChannel channel = SocketChannel.open();
				channel.connect(new InetSocketAddress("0.0.0.0", this.portNum));
				if (readOrWrite) {
					// write client
					BufferedReader readFile = new BufferedReader(new FileReader(this.fileName));
					String fileInput;
					ByteBuffer wbf = ByteBuffer.allocate(2048);
					// read the whole input file
					ArrayList<String> fileContent = new ArrayList<>();
					while ((fileInput = readFile.readLine()) != null) {
						fileContent.add(fileInput);
					}
					String newFile = "client" + clientID + "write.txt";
					BufferedWriter writer = new BufferedWriter(new FileWriter(newFile));
					int[] marks = new int[] { 0, fileContent.size() / 3, 2 * fileContent.size() / 3, fileContent.size() };
					for (int index = marks[clientID]; index < marks[clientID + 1]; index++) {
						wbf.put(("WRITE~" + fileContent.get(index) + "-").getBytes());
						wbf.flip();
						System.out.println(fileContent.get(index));
						channel.write(wbf);
						wbf.clear();
						writer.append(fileContent.get(index));
						writer.newLine();
					}
					writer.close();
					while (true) {

					}
				} else {
					//read
					channel.configureBlocking(false);
					String newFile = "client" + clientID + "read.txt";
					List<Byte> bytes = new ArrayList<>();
					ByteBuffer rbf = ByteBuffer.allocate(2048);
					String read = "READ~0-";
					for (int i = 0; i < 100; i++) {
						rbf.put(read.getBytes());
						rbf.flip();
						channel.write(rbf);
						rbf.clear();
					}
					//int count = 0;
					while (true) {
						int bytesRead = channel.read(rbf);
						if (bytesRead != -1) {
							BufferedWriter writer = new BufferedWriter(new FileWriter(newFile, true));
							rbf.flip();
							while (rbf.hasRemaining()) {
								bytes.add(rbf.get());
							}
							rbf.clear();
							while (bytes.contains((byte) '-')) {
								String data = "";
								byte b;
								Iterator<Byte> iterator = bytes.iterator();
								while (iterator.hasNext()) {
									b = iterator.next();
									if (b == '-') {
										iterator.remove();
										//count += 1;
										break;
									}
									data += (char) b;
									iterator.remove();
								}
								writer.append(data);
								writer.newLine();
							}
							writer.close();
						}
					}
				}
			}catch (IOException e) {
				
			}
		}
	}
	
}