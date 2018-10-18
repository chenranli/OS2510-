package project1;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class Client {
//	private int ID;
//	private String ip;
//	private int portNum;
//	private ServerSocket serverSocket; 
//	private ArrayList<ArrayList<Object>> connections = new ArrayList<>();
//	public Client(int id, String ip, int portNum, ServerSocket serverSocket){
//		this.ID = id;
//		this.ip = ip;
//		this.portNum = portNum;
//		this.serverSocket = serverSocket;
//	}
//	
//	public static int getServerNumber(){
//		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
//		int num = 0;
//		try {
//			num = Integer.parseInt(in.readLine());
//		} catch (NumberFormatException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return num;
//	}
	
//	public static int getServerId(){
//		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
//		int num = 0;
//		try {
//			num = Integer.parseInt(in.readLine());
//		} catch (NumberFormatException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		return num;
//	}
//	
//	public ServerSocket getClientSocket(){
//		return this.serverSocket;
//	}
//	
//	public String getip(){
//		return ip;
//	}
//	
//	public int getPort(){
//		return portNum;
//	}
//	
//	public static void sendRequest(){
//		BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
//		try {
//			String cmd = in.readLine();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
//	
//	
	
//	public void execute(){
//		try {
//			String command = "";
//			Socket s = new Socket(ip,portNum);
//			PrintWriter out = new PrintWriter(s.getOutputStream(),true);
//			BufferedReader r = new BufferedReader(new InputStreamReader(s.getInputStream()));
//			BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
//	        while ((command = stdIn.readLine()) != null) {
//	        	if(command.contains("~")){
//	        		String[] check = command.split("~");
//	            	if(check[0] == "READ"){
//	            		out.println(command);
//	                    System.out.println(r.readLine());
//	            	}
//	            	else if(check[0] == "WRITE"){
//	            		String str = "";
//	            		FileInputStream input = new FileInputStream(".\\distributed_me_requests.txt");//read the file to run
//	        			BufferedReader br = new BufferedReader(new InputStreamReader(input));
//	        			//int i = 100*(ID-1) ;
//	        			int i = 0;
//	        			
//	        			while((str = br.readLine())!=null){
//	        				if(i>=100*(ID-1)){
//	        					str = "WRITE~"+str;
//	        					out.println(str);
//	        					System.out.println(r.readLine());
//	        					if(i == ID*100){
//	        						break;
//	        					}
//	        				}
//	        				
//	        				i += 1;
//	        			}
//	            	}
//	            	else{
//	            		System.out.println("the input is invalild");
//	            	}
//	        	}
//	        	else{
//	        		System.out.println("the input is invalild");
//	        	}
//	        	
//	        }
//			s.close();
//		} catch (UnknownHostException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}//set socket, connnect server with client
//	}
	
	public static void main(String[] args) throws IOException {
        
        if (args.length != 3) {
            System.err.println(
                "Usage: java EchoClient <host name> <port number> <client id>");
            System.exit(1);
        }
 
        String hostName = args[0];
        int portNumber = Integer.parseInt(args[1]);
        int id = Integer.parseInt(args[2]);
 
        try (
            Socket echoSocket = new Socket(hostName, portNumber);
            PrintWriter out =
                new PrintWriter(echoSocket.getOutputStream(), true);
            BufferedReader in =
                new BufferedReader(
                    new InputStreamReader(echoSocket.getInputStream()));
            BufferedReader stdIn =
                new BufferedReader(
                    new InputStreamReader(System.in))
        ) {
            String userInput;
            while ((userInput = stdIn.readLine()) != null) {
	        	if(userInput.contains("~")){
	        		String[] check = userInput.split("~");
	            	if(check[0].equals("READ")){
	            		out.println(userInput);
	                    System.out.println(in.readLine());
	            	}
	            	else if(check[0].equals("WRITE")){
	            		String str = "";
	            		FileInputStream input = new FileInputStream(".\\distributed_me_requests.txt");//read the file to run
	        			BufferedReader br = new BufferedReader(new InputStreamReader(input));
	        			//int i = 100*(ID-1) ;
	        			int i = 0;
	        			
	        			while((str = br.readLine())!=null){
	        				if(i>100*(id-1)){
	        					str = "WRITE~"+str;
	        					out.println(str);
	        					System.out.println(in.readLine());
	        					if(i == id*100){
	        						break;
	        					}
	        				}
	        				
	        				i += 1;
	        			}
	            	}
	            	else{
	            		System.out.println("the input is invalild");
	            	}
	        	}
	        	else{
	        		System.out.println("the input is invalild");
	        	}
                //out.println(userInput);
                //System.out.println("echo: " + in.readLine());
            }
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + hostName);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to " +
                hostName);
            System.exit(1);
        } 
    }
}
