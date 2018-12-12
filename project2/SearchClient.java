package tinyGoogle;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

public class SearchClient {
	public static void main(String[] args) throws IOException{
		if(args.length != 2) {
			System.err.println(
					"Usage: java EchoClient <host name> <port number>");
	        System.exit(1);
		}
		
		String ip = args[0];
		int portNum = Integer.parseInt(args[1]);
		
		try
		( 
		Socket socket = new Socket(ip, portNum);
		PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in))
		)
		{
			String userInput;
			while ((userInput = stdIn.readLine()) != null) {
                out.println(userInput);
                System.out.println("echo: " + in.readLine());
            }
		}catch (UnknownHostException e) {
            System.err.println("Don't know about host " + ip);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " +
            		ip);
            System.exit(1);
        } 
	}
}