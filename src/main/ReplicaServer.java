package main;

import interfaces.ReplicaServerClientInterface;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import test.MessageNotFoundException;
import utilities.Address;
import utilities.FileContent;

public class ReplicaServer implements ReplicaServerClientInterface {
	private final int NUM_REPLICAS = 3;
	private Address currentAddress;
	private ArrayList<Address> replicaServersLocation; // list of all other replication servers
	private HashMap<String, FileContent> fileMap; // contains the current files on this server disk
	// ( can be replaced with searching on hard )

	private HashMap<String, FileContent> tempMap; // contains the files written for first time
	private HashMap<String, Address[]> replicaLocations; // contains the replica server for primary files
	private HashMap<String, LockData> lockMap; // contains the data used to lock on files while writing
	private HashMap<Long, String> transMap;
	private Registry registry;

	public ReplicaServer(Address loc) {
		this.currentAddress = loc;

		// TODO add the current replica server to registery

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
	public void write(long txnID, long msgSeqNum, FileContent data) throws RemoteException, IOException,
			NotBoundException {
		transMap.put(txnID, data.fileName);

		if (fileMap.containsKey(data.fileName) || tempMap.containsKey(data.fileName)) {
			// file exists

			if (lockMap.containsKey(data.fileName)) {
				// file is being written currently

				if (txnID != lockMap.get(data.fileName).transaction) {
					// not the same transaction so has to wait
					// TODO sort waiting threads on transaction ID (don't know how :D)
					synchronized (lockMap.get(data.fileName).lock) {
						// wait on this lock
					}
				} else {
					// the current lock increase the number of messages
					lockMap.get(data.fileName).noOfMessages++;
				}
			} else {
				lockMap.put(data.fileName, new LockData(txnID));
			}

			appendToExistingTempFile(txnID, msgSeqNum, data);

		} else {
			// file doesn't exists create new one

			if (data.isPrimary) {
				// the primary replica
				// choose new replicas
				Address[] replicas = new Address[NUM_REPLICAS];
				// choose three unique replica servers
				HashSet<Address> repl = new HashSet<Address>();
				repl.add(currentAddress);
				while (repl.size() < NUM_REPLICAS + 1) {
					Address randomAddr = replicaServersLocation.get((int) (Math.random() * 1000)
							% replicaServersLocation.size());
					repl.add(randomAddr);
				}

				int i = 0;
				for (Address addr : repl)
					if (!addr.equals(currentAddress))
						replicas[i++] = addr;

				replicaLocations.put(data.fileName, replicas);

				// send requests to replicas to write data
				data.isPrimary = false;
			} else {
				// not primary one of the replicated servers
				lockMap.put(data.fileName, new LockData(txnID));
			}

			writeNewData(txnID, msgSeqNum, data);
		}
	}

	@Override
	public FileContent read(String fileName) throws FileNotFoundException, IOException, RemoteException {
		// if file not found throw new file not found exception
		if (!fileMap.containsKey(fileName)) {
			throw new FileNotFoundException();
		}

		// if found return the file data
		return fileMap.get(fileName);
	}

	@Override
	public boolean commit(long txnID, long numOfMsgs) throws MessageNotFoundException, RemoteException,
			NotBoundException {
		String fileName = transMap.remove(txnID); // remove transaction
		LockData lockData = lockMap.remove(fileName);
		lockData.lock.notifyAll(); // Not very sure of this :D
		if (lockData.noOfMessages != numOfMsgs)
			return false;

		FileContent data = tempMap.remove(fileName);
		fileMap.put(fileName, data);
		writeDataToDisk(fileName);

		// commit remotely
		Address[] replicas = replicaLocations.get(fileName);
		boolean commited = true;
		for (int i = 0; i < replicas.length; i++) {
			ReplicaServerClientInterface replica = (ReplicaServerClientInterface) getRemoteObject(replicas[i]);
			commited &= replica.commit(txnID, numOfMsgs);
		}

		return commited;
	}

	@Override
	public boolean abort(long txnID) throws RemoteException, NotBoundException {
		String fileName = transMap.remove(txnID); // remove transaction
		LockData lockData = lockMap.remove(fileName);
		lockData.lock.notifyAll(); // Not very sure of this :D

		tempMap.remove(fileName); // remove from temp Memory

		Address[] replicas = replicaLocations.get(fileName);
		boolean aborted = true;
		for (int i = 0; i < replicas.length; i++) {
			ReplicaServerClientInterface replica = (ReplicaServerClientInterface) getRemoteObject(replicas[i]);
			aborted &= replica.abort(txnID);
		}

		return aborted;
	}

	private void writeDataToDisk(String fileName) {
		// TODO write data to disk
	}

	private void writeNewData(long txnID, long msgSeqNum, FileContent data) throws NotBoundException, IOException {
		// TODO write to the current replica

		if (data.isPrimary)
			writeRemotely(txnID, msgSeqNum, data);
	}

	private void appendToExistingTempFile(long txnID, long msgSeqNum, FileContent data) throws NotBoundException,
			IOException {
		// TODO append to the current file

		if (data.isPrimary)
			writeRemotely(txnID, msgSeqNum, data);
	}

	private void writeRemotely(long txnID, long msgSeqNum, FileContent data) throws RemoteException, IOException,
			NotBoundException {
		// write to remote replica
		Address[] replicas = replicaLocations.get(data.fileName);

		for (int j = 0; j < replicas.length; j++) {
			// get remote replica object
			ReplicaServerClientInterface replica = (ReplicaServerClientInterface) getRemoteObject(replicas[j]);

			// write at remote replica
			replica.write(txnID, msgSeqNum, data);
		}
	}

	private Remote getRemoteObject(Address serverAddr) throws RemoteException, NotBoundException {
		// get registry on the serverAdress
		Registry registry = LocateRegistry.getRegistry(serverAddr.ipAddr, serverAddr.portNumber);

		// currently we use the same reference, try to process it every time
		return registry.lookup(serverAddr.objectName);
	}

	private class LockData {
		public Object lock;
		public long transaction;
		public int noOfMessages;

		public LockData(long transactionID) {
			this.transaction = transactionID;
			lock = new Object();
			noOfMessages = 1;
		}
	}
}
