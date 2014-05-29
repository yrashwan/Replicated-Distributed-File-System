package utilities;

import java.io.Serializable;

public class WriteMsgResponse implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2676723315403138867L;
	public int transactionId;
	public int timeStamp;
	public Address loc;

	public WriteMsgResponse(int transaction, int time, Address location) {
		this.transactionId = transaction;
		this.timeStamp = time;
		this.loc = location;
	}
}
