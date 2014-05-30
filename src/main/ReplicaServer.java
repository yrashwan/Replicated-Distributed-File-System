package main;

import interfaces.ReplicaServerClientInterface;

import java.io.BufferedReader;
import java.io.File;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import test.MessageNotFoundException;
import utilities.Address;
import utilities.FileContent;

public class ReplicaServer implements ReplicaServerClientInterface {
	private final int NUM_REPLICAS = 3;
	private Address currentAddress;
	private Address[] replicaServersLocation; // list of all other replication servers
	private HashMap<String, FileContent> fileMap; // contains the current files on this server disk
	// ( can be replaced with searching on hard )

	private HashMap<String, FileContent> tempMap; // contains the files written for first time
	private HashMap<String, Address[]> replicaLocations; // contains the replica server for primary files
	private HashMap<String, LockData> lockMap; // contains the data used to lock on files while writing
	private HashMap<Long, String> transMap;
	private String directory;
	
	public ReplicaServer(Address loc, String directory) {
		File dir = new File(directory);
		dir.mkdirs();
		this.directory = directory;

		this.currentAddress = loc;
		fileMap = new HashMap<String, FileContent>();
		tempMap = new HashMap<String, FileContent>();
		replicaLocations = new HashMap<String, Address[]>();
		lockMap = new HashMap<String, LockData>();
		transMap = new HashMap<Long, String>();
		
		System.out.println("File Map : " + fileMap.size());
		System.out.println("Temp Map : " + tempMap.size());
		System.out.println("Replica Locations : " + replicaLocations.size());
		System.out.println("Lock Map : " + lockMap.size());
		System.out.println("Trans Map : " + transMap.size());
		

		// in the start we don't call ReplicaServer, MasterServer directly .. instead we call RmiReplicaServer,
		// RmiMasterServer respectively

		// read replica servers location from file
		try {
			BufferedReader br = new BufferedReader(new FileReader("replicated.txt"));
			int n = Integer.parseInt(br.readLine());
			replicaServersLocation = new Address[n];
			for (int i = 0; i < n; i++) {
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
	public void write(long txnID, long msgSeqNum, FileContent data) throws RemoteException, IOException,
			NotBoundException {
		transMap.put(txnID, data.fileName);

		if (fileMap.containsKey(data.fileName) || tempMap.containsKey(data.fileName)) {
			// file exists
			if (lockMap.get(data.fileName).transaction == txnID) {

				// file is being written currently by the same transaction

				// increase the number of messages
				lockMap.get(data.fileName).noOfMessages.incrementAndGet();
			} else {
				// new write request
				try {
					lockMap.get(data.fileName).lock.acquire();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				lockMap.get(data.fileName).transaction = txnID;
				// reset number of messages
				lockMap.get(data.fileName).noOfMessages.set(1);
			}

			appendToExistingTempFile(txnID, msgSeqNum, data);

		} else {
			// file doesn't exists create new one
			lockMap.put(data.fileName, new LockData(txnID));
			try {
				lockMap.get(data.fileName).lock.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			if (data.isPrimary) {
				// the primary replica
				// choose new replicas
				Address[] replicas = new Address[NUM_REPLICAS];
				// choose three unique replica servers
				HashSet<Address> repl = new HashSet<Address>();
				repl.add(currentAddress);
				while (repl.size() < NUM_REPLICAS + 1) {
					Address randomAddr = replicaServersLocation[(int) (Math.random() * 1000)
							% replicaServersLocation.length];
					repl.add(randomAddr);
				}

				int i = 0;
				for (Address addr : repl)
					if (!addr.objectName.equals(currentAddress.objectName))
						replicas[i++] = addr;

				replicaLocations.put(data.fileName, replicas);

				// send requests to replicas to write data
			} else {
				// not primary one of the replicated servers
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
		System.out.println("REPLICA : " + currentAddress.toString() + ", Commit File : " + fileName);
		LockData lockData = lockMap.get(fileName);

		System.out.println("REPCLIA : NUM OF MESSAGES : " + lockData.noOfMessages.get());

		if (lockData.noOfMessages.get() != numOfMsgs) {
			System.out.println("REPLICA : CURRENT FILE : \n" + tempMap.get(fileName));
			lockData.lock.release();
			return false;
		}

		FileContent data = tempMap.remove(fileName);
		fileMap.put(fileName, data);
		writeDataToDisk(fileName);

		// commit remotely
		Address[] replicas = replicaLocations.get(fileName);
		boolean commited = true;
		if (replicas != null) // primary replica
			for (int i = 0; i < replicas.length; i++) {
				ReplicaServerClientInterface replica = (ReplicaServerClientInterface) getRemoteObject(replicas[i]);
				commited &= replica.commit(txnID, numOfMsgs);
			}

		lockData.lock.release();
		return commited;
	}

	@Override
	public boolean abort(long txnID) throws RemoteException, NotBoundException {
		String fileName = transMap.remove(txnID); // remove transaction
		LockData lockData = lockMap.get(fileName);

		tempMap.remove(fileName); // remove from temporary Memory

		Address[] replicas = replicaLocations.get(fileName);
		boolean aborted = true;
		if (replicas != null) // primary replica
			for (int i = 0; i < replicas.length; i++) {
				ReplicaServerClientInterface replica = (ReplicaServerClientInterface) getRemoteObject(replicas[i]);
				aborted &= replica.abort(txnID);
			}

		lockData.lock.release();
		return aborted;
	}

	private void writeDataToDisk(String fileName) {
		// TODO write data to disk
	}

	private void writeNewData(long txnID, long msgSeqNum, FileContent data) throws NotBoundException, IOException {
		tempMap.put(data.fileName, data);
		System.out.println("REPLICA : " + currentAddress.toString() + ", Create New File : " + data.fileName);

		if (data.isPrimary) {
			data.isPrimary = false;
			writeRemotely(txnID, msgSeqNum, data);
		}
	}

	private void appendToExistingTempFile(long txnID, long msgSeqNum, FileContent data) throws NotBoundException,
			IOException {
		System.out.println("REPLICA : " + currentAddress.toString() + ", Append to Existing File : " + data.fileName);

		if (!tempMap.containsKey(data.fileName)) {
			tempMap.put(data.fileName, new FileContent(fileMap.get(data.fileName)));
		}

		tempMap.get(data.fileName).data += data.data; // append the string to the existing one

		if (data.isPrimary) {
			data.isPrimary = false;
			writeRemotely(txnID, msgSeqNum, data);
		}
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
		public Semaphore lock;
		public long transaction;
		public AtomicInteger noOfMessages;

		public LockData(long transactionID) {
			this.transaction = transactionID;
			lock = new Semaphore(1);
			noOfMessages = new AtomicInteger(1);
		}
	}
}
