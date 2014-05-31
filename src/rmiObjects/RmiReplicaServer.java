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
	
	public RmiReplicaServer(Address loc, Address masterAddress, String directory) throws RemoteException {
		this.objectName = loc.objectName;
		obj_strong_ref = new ReplicaServer(loc, masterAddress, directory);

		// option#1 ============ using getRegistry
		LocateRegistry.createRegistry(loc.portNumber);
		registry = LocateRegistry.getRegistry(loc.ipAddr, loc.portNumber);
		ReplicaServerClientInterface stub;
		try {
			stub = (ReplicaServerClientInterface) UnicastRemoteObject.exportObject(
					obj_strong_ref, loc.portNumber);
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
	public static void main(String[] args) throws NumberFormatException, RemoteException {
		try{
		System.out.println("RMIReplicateServer: Receiving Args "
				+ Arrays.toString(args));
		new RmiReplicaServer(new Address(args[0],
				Integer.parseInt(args[1]), args[2]),
				new Address(args[3], Integer.parseInt(args[4]), args[5]),
				"ServersData/Replica" + args[6] + "/");
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
}
