package utilities;

public class WriteMsgResponse {
	public int transactionId;
	public int timeStamp;
	public Address loc;

	public WriteMsgResponse(int transaction, int time, Address location) {
		this.transactionId = transaction;
		this.timeStamp = time;
		this.loc = location;
	}
}
