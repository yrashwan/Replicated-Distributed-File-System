package main;

import interfaces.MasterServerClientInterface;
import interfaces.ReplicaServerClientInterface;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;

import utilities.Address;
import utilities.MethodsUtility;
import utilities.WriteMsgResponse;

public class MasterServer implements MasterServerClientInterface {

	private HashMap<String, Address[]> fileMap; // carry the location of primary replica Server
	private Address[] replicaServersLocation;
	private int currentTime;
	private int currentTransaction;
	private final int NUM_REPLICAS = 4;
	private String directory;
	private final String METADATA = "metaData.txt";

	public MasterServer(String directory) {
		File dir = new File(directory);
		dir.mkdirs();
		this.directory = directory;

		fileMap = MethodsUtility.readMetaData(this.directory + METADATA);
		currentTime = 1;
		currentTransaction = 1;

		// read replica servers location from file
		try {
			BufferedReader br = new BufferedReader(new FileReader("replicated.txt"));
			int n = Integer.parseInt(br.readLine());
			replicaServersLocation = new Address[n];
			for (int i = 0; i < n; i++) {
				String s = br.readLine();
				String[] tokens = s.split(" ");
				replicaServersLocation[i] = (new Address(tokens[0], Integer.parseInt(tokens[1]),
						tokens[2]));
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Address read(String fileName) throws FileNotFoundException, IOException, RemoteException {
		if (!fileMap.containsKey(fileName)) {
			// if file not found throw FileNotFoundException
			throw new FileNotFoundException();
		} else {
			// if file exists return its location
			return fileMap.get(fileName)[(int) (Math.random() * 1000) % NUM_REPLICAS];
		}
	}

	@Override
	public WriteMsgResponse write(String fileName) throws RemoteException, IOException,
			NotBoundException {
		if (!fileMap.containsKey(fileName)) {
			// if file not found create metaData for it in the MainServer
			Address[] replicas = getRandomReplicas();

			Address[] otherReplicas = new Address[replicas.length - 1];
			for (int i = 1; i < replicas.length; i++)
				otherReplicas[i - 1] = replicas[i];

			ReplicaServerClientInterface primaryReplica = (ReplicaServerClientInterface) MethodsUtility
					.getRemoteObject(replicas[0]);
			primaryReplica.addReplicas(fileName, otherReplicas);

			fileMap.put(fileName, replicas);
			MethodsUtility.writeMetaData(directory + METADATA, fileMap);
		}

		WriteMsgResponse response = new WriteMsgResponse(currentTransaction++, currentTime++,
				fileMap.get(fileName)[0]);
		return response;
	}

	private Address[] getRandomReplicas() {
		Address[] replicas = new Address[NUM_REPLICAS];
		// choose three unique replica servers
		HashSet<Address> repl = new HashSet<Address>();
		while (repl.size() < NUM_REPLICAS) {
			Address randomAddr = replicaServersLocation[(int) (Math.random() * 1000)
					% replicaServersLocation.length];
			repl.add(randomAddr);
		}

		int i = 0;
		for (Address addr : repl)
			replicas[i++] = addr;

		return replicas;
	}

	@Override
	public void abort(String fileName) throws RemoteException, NotBoundException {
		fileMap.remove(fileName);
		MethodsUtility.writeMetaData(directory + METADATA, fileMap);
	}
}
