package indexSearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Server{
	private static class ValueComparator implements Comparator<Map.Entry<Integer, Integer>>{
		public int compare(Map.Entry<Integer, Integer> m, Map.Entry<Integer, Integer> n) {
			return n.getValue()-m.getValue();
		}
	}
	
	private final int portNum;
	public static LinkedHashSet<Integer> ans = new LinkedHashSet<Integer>();
	//public static LinkedHashMap<String,HashMap<Integer,Integer>> tmp = new LinkedHashMap<String,HashMap<Integer,Integer>>();
	private static List<Map.Entry<Integer,Integer>> list = new ArrayList<>();
	private List<String> fileList = new ArrayList();
	Server(int pn){
		this.portNum = pn;
	}
	
	public void start() {
		try 
		{
			ServerSocket serverSocket = new ServerSocket(this.portNum);//begin a new Server
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
								Server.ans.clear();
								//Server.tmp.clear();
								Server.list.clear();
								//HashSet<Integer> ans = new HashSet<Integer>();
								//HashSet<Integer> t = new HashSet<Integer>();
								BlockingQueue queue = new LinkedBlockingQueue();
								ExecutorService threadPool = Executors.newFixedThreadPool(10);
								threadPool.submit(new Helper(1,inputLine,queue,fileList));
								threadPool.submit(new Helper(2,inputLine,queue,fileList));
								threadPool.submit(new Helper(3,inputLine,queue,fileList));
								threadPool.submit(new Helper(4,inputLine,queue,fileList));
								threadPool.submit(new Helper(5,inputLine,queue,fileList));
								threadPool.submit(new Helper(6,inputLine,queue,fileList));
								threadPool.submit(new Helper(7,inputLine,queue,fileList));
								threadPool.submit(new Helper(8,inputLine,queue,fileList));
								threadPool.submit(new Helper(9,inputLine,queue,fileList));	
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
									"Exception caught when trying to listen on port " + portNum + " or listening for a connection");
							System.out.println(e.getMessage());
						}
						
					}
				});
			}	
		} catch (IOException e) {
			System.out.println(
					"Exception caught when trying to listen on port " + this.portNum + " or listening for a connection");
			System.out.println(e.getMessage());
		}
	}
	
	public void rank(BlockingQueue q) {
		//for()
		LinkedHashMap<String,HashMap<Integer,Integer>> tmp = new LinkedHashMap<String,HashMap<Integer,Integer>>();
		while(!q.isEmpty()) {
			try {
				tmp.putAll((HashMap<String,HashMap<Integer,Integer>>)q.take());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//System.out.println(tmphash);
		System.out.println(tmp);
		LinkedHashMap<Integer,Integer> output = new LinkedHashMap<Integer,Integer>();
		for(HashMap<Integer,Integer> value: tmp.values()) {
			//System.out.println("value"+value);
			for(Map.Entry<Integer, Integer> entry: value.entrySet()) {
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
		//System.out.println(output);
		//System.out.println(list);
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 1) {
			System.err.println("Usage: java EchoServer <port number>");
			System.exit(1);
		}

		int portNum = Integer.parseInt(args[0]);
		Server s = new Server(portNum);
		s.start();
	}
}
