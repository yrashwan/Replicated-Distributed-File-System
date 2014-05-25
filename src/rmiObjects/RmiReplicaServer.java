package rmiObjects;

import interfaces.ReplicaServerClientInterface;

import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;

import main.ReplicaServer;

import utilities.Address;

public class RmiReplicaServer {

	// in order not to be garbage collected, should be strong reference
	public static ReplicaServerClientInterface obj_strong_ref;

	private static Registry registry;
	private final String objectName;
	
	public RmiReplicaServer(String serverAddress,int portNumber, String objectName,Address loc) throws RemoteException {

		this.objectName = objectName;
		obj_strong_ref = new ReplicaServer(loc);

		// option#1 ============ using getRegistry
		LocateRegistry.createRegistry(portNumber);
		registry = LocateRegistry.getRegistry(serverAddress, portNumber);
		ReplicaServerClientInterface stub;
		try {
			stub = (ReplicaServerClientInterface) UnicastRemoteObject.exportObject(
					obj_strong_ref, portNumber);
		} catch (ExportException e) {
			stub = (ReplicaServerClientInterface) UnicastRemoteObject.toStub(obj_strong_ref);
		}
		registry.rebind(objectName, stub);
		System.out.println("*** registry " + Arrays.toString(registry.list()));

		System.out.println("Server bound");
	}

	public void end() throws RemoteException, MalformedURLException,
			NotBoundException {
		System.out.println(Arrays.toString(registry.list()));
		registry.unbind(objectName);
		System.out.println("server ended");
	}
}