package main;

import interfaces.MasterServerClientInterface;
import interfaces.ReplicaServerClientInterface;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;

import test.MessageNotFoundException;
import utilities.Address;
import utilities.FileContent;
import utilities.WriteMsgResponse;

public class Client {
	private final MasterServerClientInterface masterServer;

	public Client(Address mainServerAddress) throws RemoteException, NotBoundException {
		this.masterServer = (MasterServerClientInterface) getRemoteObject(mainServerAddress);
	}

	public void run() throws FileNotFoundException, RemoteException, IOException {
		masterServer.read("hello");
	}

	public FileContent read(String fileName) throws FileNotFoundException, RemoteException, IOException,
			NotBoundException {
		// get the address of primary replicated server
		Address primaryReplicaAddress = masterServer.read(fileName);

		// get remote primary replica object
		ReplicaServerClientInterface primaryReplica = (ReplicaServerClientInterface) getRemoteObject(primaryReplicaAddress);

		// read file
		FileContent file = primaryReplica.read(fileName);
		return file;
	}

	public void write(String fileName, ArrayList<FileContent> data) throws FileNotFoundException, RemoteException,
			IOException, NotBoundException, MessageNotFoundException {
		// get the address of primary replicated server
		WriteMsgResponse masterResponse = masterServer.write(fileName);

		// get remote primary replica object
		ReplicaServerClientInterface primaryReplica = (ReplicaServerClientInterface) getRemoteObject(masterResponse.loc);

		for (int i = 0; i < data.size(); i++)
			primaryReplica.write(masterResponse.transactionId, i, data.get(i));

		boolean finishedSuccessfully = primaryReplica.commit(masterResponse.transactionId, data.size());
	}

	private Remote getRemoteObject(Address serverAddr) throws RemoteException, NotBoundException {
		// get registry on the serverAdress
		Registry registry = LocateRegistry.getRegistry(serverAddr.ipAddr, serverAddr.portNumber);

		// currently we use the same reference, try to process it every time
		return registry.lookup(serverAddr.objectName);
	}
}
