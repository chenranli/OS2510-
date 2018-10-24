package mutualExclusion;

import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.*;

public class Client{
	
	public static void main(String[] args) throws IOException{
		
		String filePath = args[0];
		System.out.println("file path : " + filePath);
		String hostName = args[1];
		int portNumber = Integer.parseInt(args[2]);
		
		//whether this is a reading client or writing client
		String clientType = args[3];
		int clientID = Integer.parseInt(args[4]);
		
		File file = new File(filePath);
		try {
			BufferedReader readFile = new BufferedReader(new FileReader(file));
			SocketChannel channel = SocketChannel.open();
			channel.connect(new InetSocketAddress(hostName, portNumber));
			//writing client 
			if (clientType.equals("WRITE") || clientType.equals("write")) {
				String fileInput;
				ByteBuffer wbf = ByteBuffer.allocate(2048);
				// read the whole input file
				ArrayList<String> fileContent = new ArrayList<>();
				while ((fileInput = readFile.readLine()) != null) {
					fileContent.add(fileInput);
				}
				String newFile = "client" + clientID + "write.txt";
				BufferedWriter writer = new BufferedWriter(new FileWriter(newFile));
				int[] marks = new int[] { 0, fileContent.size() / 6, 2 * fileContent.size() / 6,
						3 * fileContent.size() / 6, 4 * fileContent.size() / 6, 5 * fileContent.size() / 6,
						fileContent.size() };
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
				BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
				while (stdIn.readLine() != null) {
					
				}
			}else {
				// reading client
				channel.configureBlocking(false);
				System.out.println("This is a reading client.");
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
		} catch (UnknownHostException e) {
			System.err.println("Don't know about host" + hostName);
			System.exit(1);
		} catch (IOException e) {
			System.err.println("Couldn't get I/O for the connection to " + hostName);
			System.exit(1);
		}
	}
}