package mutualExclusion;

import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

public class OtherServer {
	private final int serverID;
	private final List<Byte> input = new ArrayList<>();
	private final SocketChannel channel;
	
	public OtherServer(int serverID, SocketChannel channel) {
		this.serverID = serverID;
		this.channel = channel;
	}
	
	public int getID() {
		return this.serverID;
	}
	
	public SocketChannel getChannel() {
		return this.channel;
	}
	
	public List<Byte> addInput(byte b) {
		this.input.add(b);
		return this.input;
	}
	
	public List<Byte> getInput() {
		return this.input;
	}
	
}
