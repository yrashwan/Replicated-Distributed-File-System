package interfaces;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import test.MessageNotFoundException;
import utilities.FileContent;

public interface ReplicaServerClientInterface extends Remote {
	/**
	 * 
	 * @param txnID
	 *            : the ID of the transaction to which this message relates
	 * @param msgSeqNum
	 *            : the message sequence number. Each transaction starts with
	 *            message sequence number 1.
	 * @param data
	 *            : data to write in the file
	 * @return message with required info
	 * @throws IOException
	 * @throws RemoteException
	 * @throws NotBoundException 
	 */	
	public void write(long txnID, long msgSeqNum, FileContent data)
			throws RemoteException, IOException, NotBoundException;
	
	public FileContent read(String fileName) throws FileNotFoundException,
	IOException, RemoteException;
	
	/**
	 * 
	 * @param txnID
	 *            : the ID of the transaction to which this message relates
	 * @param numOfMsgs
	 *            : Number of messages sent to the server
	 * @return true for acknowledgment
	 * @throws MessageNotFoundException
	 * @throws RemoteException
	 * @throws NotBoundException 
	 */
	public boolean commit(long txnID, long numOfMsgs)
			throws MessageNotFoundException, RemoteException, NotBoundException;
	
	/**
	 * * @param txnID: the ID of the transaction to which this message relates
	 * 
	 * @return true for acknowledgment
	 * @throws RemoteException
	 * @throws NotBoundException 
	 */
	public boolean abort(long txnID) throws RemoteException, NotBoundException;
	
	/**
	 * add the replicas for the primary replica one
	 * @param fileName
	 * @param address
	 */
	public void addReplicas(String fileName, utilities.Address[] address)throws RemoteException, NotBoundException;
}
