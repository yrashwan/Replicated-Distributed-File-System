package main;

import interfaces.MasterServerClientInterface;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;

import utilities.Address;
import utilities.WriteMsgResponse;

public class MasterServer implements MasterServerClientInterface {

	private HashMap<String, Address> fileMap; // carry the location of primary replica Server
	private ArrayList<Address> replicaServersLocation;
	private int currentTime;
	private int currentTransaction;

	public MasterServer() {
		// TODO read this map from file
		fileMap = new HashMap<String, Address>();
		currentTime = 1;
		currentTransaction = 1;

		// read replica servers location from file
		replicaServersLocation = new ArrayList<Address>();
		try {
			BufferedReader br = new BufferedReader(new FileReader("replicated.txt"));
			while (br.ready()) {
				String s = br.readLine();
				String[] tokens = s.split(" ");
				replicaServersLocation.add(new Address(tokens[0], Integer.parseInt(tokens[1]), tokens[2]));
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
		int index = (int) (Math.random() * 1000) % replicaServersLocation.size();
		return replicaServersLocation.get(index);
	}

}
