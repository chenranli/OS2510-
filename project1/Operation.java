package mutualExclusion;

import java.nio.channels.SocketChannel;

public class Operation {
	private final int serverID;
	private final String oprType;
	private final String update;
	private final int timeStamp;
	private final SocketChannel channel;
	// whether this operation has been performed
	private boolean isDone = false;

	public Operation(int serverID, String oprType, String update, int timeStamp, SocketChannel channel) {
		this.serverID = serverID;
		this.oprType = oprType;
		this.update = update;
		this.timeStamp = timeStamp;
		this.channel = channel;
	}
	
	public int getID() {
		return this.serverID;
	}

	public String getOprType() {
		return this.oprType;
	}

	public String getUpdate() {
		return this.update;
	}

	public SocketChannel toClient() {
		return this.channel;
	}

	public void changeStat() {
		this.isDone = true;
	}

	public boolean checkStat() {
		return this.isDone;
	}

	public int getTime() {
		return this.timeStamp;
	}
}
