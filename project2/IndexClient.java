package tinyGoogle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;

public class IndexClient {

	public static void main(String[] args) {
		String serverIP = args[0];
		int serverPort = Integer.parseInt(args[1]);
		try (
			BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
			)
		{
			SocketChannel sChannel = SocketChannel.open();
			sChannel.connect(new InetSocketAddress(serverIP, serverPort));
			sChannel.configureBlocking(false);
			String userInput;
			while ((userInput = stdIn.readLine()) != null) {
				//new Client().parseUserInput(userInput);
				ByteBuffer sbuf = ByteBuffer.allocate(1024);
				sbuf.put(("INDEX~" + userInput + "-").getBytes());
				sbuf.flip();
				sChannel.write(sbuf);
				sbuf.clear();
				ArrayList<Byte> bytes = new ArrayList<>();
				while (true) {
					int bytesRead = sChannel.read(sbuf);
					if (bytesRead != -1) {
						sbuf.flip();
						while (sbuf.hasRemaining()) {
							byte b = sbuf.get();
							bytes.add(b);
							System.out.println("~~~~~~~~we received " + ((Byte)b).toString() + "~~~~~~~~~~~~~~");
						}
						if (bytes.contains((byte)'-')) {
							System.out.println("success");
							break;
						}else if (bytes.contains((byte)'?')) {
							System.out.println("duplicated file");
							break;
						}else if (bytes.contains((byte)'!')) {
							System.out.println("indexing failed");
							break;
						}
					}
					sbuf.clear();
				}
			}
		}catch (IOException e) {
			
		}
		
	}
}
