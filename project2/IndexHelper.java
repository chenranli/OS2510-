package tinyGoogle;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IndexHelper {
	private final ArrayList<SocketChannel> channelList = new ArrayList<>();
	private final ArrayList<Byte> sInput = new ArrayList<>(); 
	private final ArrayList<Byte> mInput = new ArrayList<>(); 
	private final ArrayList<String> indexFilePath = new ArrayList<>();
	private ArrayList<ArrayList<Pair>> index = new ArrayList<>();
	private String fileID = "";
	
	public static void main(String[] args) {
		
		String serverIP = args[0];
		int serverPort = Integer.parseInt(args[1]);
		IndexHelper helper = new IndexHelper();
		
		try {
			//channel connecting to server
			SocketChannel sChannel = SocketChannel.open();
			sChannel.connect(new InetSocketAddress(serverIP, serverPort));
			sChannel.configureBlocking(false);
			ByteBuffer sbuf = ByteBuffer.allocate(2048);
			ByteBuffer mbuf = ByteBuffer.allocate(2048);		
			String FINISHINDEXING = "1";
			String FINISHWRITING = "2";
			sbuf.put((byte)'-');
			sbuf.flip();
			sChannel.write(sbuf);
			sbuf.clear();
			while (true) {
				int bytesRead = sChannel.read(sbuf);
				if (bytesRead != -1) {
					sbuf.flip();
					while (sbuf.hasRemaining()) {
						helper.sInput.add(sbuf.get());
					}
					sbuf.clear();
					helper.parseServerInput();
				}
				if (helper.channelList.size() != 0) {
					bytesRead = helper.channelList.get(0).read(mbuf);
					if (bytesRead != -1) {
						mbuf.flip();
						while (mbuf.hasRemaining()) {
							helper.mInput.add(mbuf.get());
						}						
					}
					mbuf.clear();
					int value = helper.parseMasterInput();
					if (value == 1) {
						//send "finish-indexing"
						System.out.println("~~~~~~~~~~~send finish indexing message~~~~~~~~~~");
						System.out.println();
						System.out.println();
						mbuf.put((FINISHINDEXING + "-").getBytes());
						mbuf.flip();
						helper.channelList.get(0).write(mbuf);
						mbuf.clear();						
					}else if (value == 2) {
						//send "finish-write"
						//remove channel
						System.out.println("------------just finished writing-----------");
						System.out.println();
						mbuf.put((FINISHWRITING + "-").getBytes());
						mbuf.flip();
						helper.channelList.get(0).write(mbuf);
						mbuf.clear();	
						helper.channelList.clear();
						System.out.println("----------------already sent finish-writing message-------------");
						System.out.println();
					}	
				}
			}			
		} catch(IOException e) {
			
		}
		

	}

	private void addMaster(String masterIP, int masterPort) {
		try {
			SocketChannel channel = SocketChannel.open();
			channel.connect(new InetSocketAddress(masterIP, masterPort));
			channel.configureBlocking(false);
			this.channelList.add(channel);
		} catch(IOException e) {
		
		}	
	}
	
	private void parseServerInput() {
		if (this.sInput.contains((byte) '-')) {
			String opr = "";
			for (byte b : this.sInput) {
				if (b == '-') {
					break;
				}
				opr += (char)b;
			}
			this.sInput.clear();
			//ip~port~-
			String[] masterInfo = opr.split("~");
			addMaster(masterInfo[0], Integer.parseInt(masterInfo[1]));
			System.out.println("received master information!!!~~~~~~~~~~~~~~" + masterInfo[0] + "~~~~~~~~~~~" + masterInfo[1]);
		}
	}
	
	private int parseMasterInput() throws IOException{
		int value = 0;
		if (this.mInput.contains((byte) '-')) {
			String opr = "";
			for (byte b : this.mInput) {
				if (b == '-') {
					break;
				}
				opr += (char)b;
			}
			this.mInput.clear();
			//filePath~startingLine~endingLine- or ALLOW~indexFilePath~p~p~p~p~...~p-
			String[] oprInfo = opr.split("~");
			if (oprInfo[0].equals("ALLOW")) {
				System.out.println();
				System.out.println("receive ALLOW message from master : ");
				System.out.println(opr);
				System.out.println();
				value = 2;
				ArrayList<ArrayList<Pair>> old = new ArrayList<>();
				for (int i = 1; i < 28; i ++) {
					this.indexFilePath.add(oprInfo[i]);
					old.add(parseIndexFile(oprInfo[i]));
				}
				ArrayList<ArrayList<Pair>> newIndex = merge(old);
				
//				ArrayList<Pair> testList = newIndex.get(0);//one file
//				Pair pp = testList.get(0);//one line in one file
//				ArrayList<Pair> tuples = (ArrayList)pp.attrb2;//tuples in one line
//				for (Pair tuple : tuples) {
//					System.out.println("pair : " + tuple.attrb1 + "|||||" + tuple.attrb2);
//				}
//				
				write(newIndex, this.indexFilePath);
				this.indexFilePath.clear();
				this.index.clear();
				System.out.println();
				System.out.println("write to flie");
				System.out.println();
			}else {
				value = 1;
				this.fileID = oprInfo[0];
				System.out.println("-------------receive indexing message from master");
				System.out.println(opr);
				System.out.println();
				indexing(oprInfo[0], Integer.parseInt(oprInfo[1]), Integer.parseInt(oprInfo[2]));
			}
			this.mInput.clear();
		}
		return value;
	}	
	
	//indexing, add result to local data structure
	private void indexing(String filePath, int starting, int ending) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
		String fileInput;
		ArrayList<Pair> wordList = new ArrayList<>();
		int lines = 0;
		while ((fileInput = reader.readLine()) != null) {
			lines += 1;
			if (lines >= starting && lines <= ending) {
				String line = fileInput.replaceAll("[\\W]"," ");
				String[] words = line.split("\\s+");
				for (String word : words) {
					if (!word.equals("")) {
						boolean inList = false;
						for (Pair p : wordList) {
							if (p.attrb1.equals(word)) {
								inList = true;
								p.attrb2 = "" + (Integer.parseInt((String)p.attrb2) + 1);
								break;
							}
						}
						if (!inList) {
							wordList.add(new Pair(word, "1"));
						}
					}
				}
			}
		}
		//categorize word/frequency pair according to starting letter of word
		ArrayList<ArrayList<Pair>> newWordList = new ArrayList<>(27);
		for (int i = 0; i < 27; i ++) {
			newWordList.add(new ArrayList<Pair>());
		}
		for (Pair p : wordList) {
			int charValue = (int)p.attrb1.charAt(0);
			int difference;
			if (charValue >= 97 && charValue <= 122) {//word starts with a~z
				difference = charValue - 97;
				newWordList.get(difference).add(p);
			}else if (charValue >= 65 && charValue <= 90) {
				difference = charValue - 65;
				newWordList.get(difference).add(p);
			}else {//word starts with A~Z
				newWordList.get(26).add(p);
			}
		}		
		//到这里没有问题!!!!!
		System.out.println("");
		this.index = newWordList;//this index List<List<Pair(word, frq)>>
		reader.close();
	}
	
	private void print(ArrayList<ArrayList<Pair>> a) {
		ArrayList<Pair> test0 = a.get(0);//one file
		for (Pair ppp : test0) {
			System.out.println(ppp.attrb1 + "~~~~~~~~~~~~" + ppp.attrb2);
		}
	}
	
	private ArrayList<ArrayList<Pair>> merge(ArrayList<ArrayList<Pair>> old) {
		ArrayList<ArrayList<Pair>> newIndex = new ArrayList<>();
		for (int i = 0; i < 27; i ++) {//for each file
			for (Pair p : this.index.get(i)) {//for each (word, frq)
				String word = p.attrb1;
				boolean containWord = false;
				boolean containFile = false;
				int value = Integer.parseInt((String)p.attrb2);
				for (Pair line : old.get(i)) {// word  (() () () () () ...)
					if (line.attrb1.equals(word)) {
						containWord = true;
						List<Pair> postingList = (List<Pair>)line.attrb2;
						int position = postingList.size();
						for (Pair pair : postingList) {
							if (pair.attrb1.equals(this.fileID)) {
								containFile = true;
								int count = (int)pair.attrb2 + Integer.parseInt((String)p.attrb2);
								pair.attrb2 = count;
								break;
							}
							if ((int)pair.attrb2 < value) {
								position = postingList.indexOf(pair);
								break;
							}
						}
						if (!containFile) {
							postingList.add(position, new Pair(this.fileID, value + ""));
							break;
						}
					}
				}
				if (!containWord) {
					//add (word, [id, frq]) to old 
					List<Pair> newPostingList = new ArrayList<>();
					newPostingList.add(new Pair(this.fileID, value + ""));
					old.get(i).add(new Pair(word, newPostingList));
				}
			}			
		}
		return old;
	}
	
	private ArrayList<Pair> parseIndexFile(String filePath) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
		String fileInput;
		ArrayList<Pair> oldIndex = new ArrayList<>();
		while ((fileInput = reader.readLine()) != null) {
			String[] tokens = fileInput.split(" ");
			String term = tokens[0];
			List postingList = new ArrayList<Pair>();
			for (int i = 1; i < tokens.length; i++) {
				String[] pair = tokens[i].split(",");
				Pair idFrq = new Pair(pair[0], Integer.parseInt(pair[1]));
				postingList.add(idFrq);
			}
			Pair entry = new Pair(term, postingList);
			oldIndex.add(entry);
		}
		reader.close();
		return oldIndex;
	}
	
	
	private void write(ArrayList<ArrayList<Pair>> newIndex, ArrayList<String> to) throws IOException{
		//1.write to shared file
		//2.send success to master
		//3.empty channelList
		for (int i = 0; i < 27; i ++) {
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(to.get(i))));
			for (Pair p : newIndex.get(i)) {
				String line = "";
				line += p.attrb1;
				List<Pair> list = (List)p.attrb2;
				for (Pair pair : list) {
					line = line + " " + pair.attrb1 + "," + pair.attrb2;
				}
				writer.append(line);
				writer.newLine();
			}
			writer.close();
		}
	}

	private class Pair{
		private final String attrb1;
		private Object attrb2;
		
		private Pair(String attrb1, Object attrb2) {
			this.attrb1 = attrb1;
			this.attrb2 = attrb2;
		}
	}
}
