package rmiObjects;

import interfaces.MasterServerClientInterface;

import java.net.MalformedURLException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.ExportException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;

import main.MasterServer;

import utilities.Address;

public class RmiMasterServer {

	// in order not to be garbage collected, should be strong reference
	public static MasterServerClientInterface obj_strong_ref;

	private static Registry registry;
	private final String objectName;

	public RmiMasterServer(Address loc) throws RemoteException {

		this.objectName = loc.objectName;
		obj_strong_ref = new MasterServer();

		// option#1 ============ using getRegistry
		LocateRegistry.createRegistry(loc.portNumber);
		registry = LocateRegistry.getRegistry(loc.ipAddr, loc.portNumber);
		MasterServerClientInterface stub;
		try {
			stub = (MasterServerClientInterface) UnicastRemoteObject.exportObject(obj_strong_ref, loc.portNumber);
		} catch (ExportException e) {
			stub = (MasterServerClientInterface) UnicastRemoteObject.toStub(obj_strong_ref);
		}
		registry.rebind(objectName, stub);
		System.out.println("*** registry " + Arrays.toString(registry.list()));

		System.out.println("Server bound");
	}

	public void end() throws RemoteException, MalformedURLException, NotBoundException {
		System.out.println(Arrays.toString(registry.list()));
		registry.unbind(objectName);
		System.out.println("server ended");
	}
}
