package interfaces;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import utilities.Address;
import utilities.WriteMsgResponse;

public interface MasterServerClientInterface extends Remote {
	/**
	 * Read file from server
	 * 
	 * @param fileName
	 * @return the addresses of  of its primary replica
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws RemoteException
	 */
	public Address read(String fileName) throws FileNotFoundException,
			IOException, RemoteException;

	/**
	 * Start a new write transaction
	 * 
	 * @param fileName
	 * @return the required info
	 * @throws RemoteException
	 * @throws IOException
	 */
	public WriteMsgResponse write(String fileName) throws RemoteException, IOException, NotBoundException;

}
