package main;

import interfaces.MasterServerClientInterface;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.HashMap;

import utilities.Address;
import utilities.WriteMsgResponse;

public class MasterServer implements MasterServerClientInterface {

	private HashMap<String, Address> fileMap; // carry the location of primary replica Server
	private Address[] replicaServersLocation;
	private int currentTime;
	private int currentTransaction;
	private String directory;

	public MasterServer(String directory) {
		File dir = new File(directory);
		dir.mkdirs();
		this.directory = directory;
		
		// TODO read this map from file
		fileMap = new HashMap<String, Address>();
		currentTime = 1;
		currentTransaction = 1;

		// read replica servers location from file
		try {
			BufferedReader br = new BufferedReader(new FileReader("replicated.txt"));
			int n = Integer.parseInt(br.readLine());
			replicaServersLocation = new Address[n];
			for(int i = 0; i < n; i++){
				String s = br.readLine();
				String[] tokens = s.split(" ");
				replicaServersLocation[i] = (new Address(tokens[0], Integer.parseInt(tokens[1]), tokens[2]));
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
			return fileMap.get(fileName);
		}
	}

	@Override
	public WriteMsgResponse write(String fileName) throws RemoteException, IOException {
		if (!fileMap.containsKey(fileName)) {
			// if file not found create metaData for it in the MainServer
			fileMap.put(fileName, getRandomReplicaServerLoc());
		}

		WriteMsgResponse response = new WriteMsgResponse(currentTransaction++, currentTime++, fileMap.get(fileName));
		return response;
	}

	private Address getRandomReplicaServerLoc() {
		int index = (int) (Math.random() * 1000) % replicaServersLocation.length;
		return replicaServersLocation[index];
	}

}
