package main;

import interfaces.MasterServerClientInterface;
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
import java.util.Map;
import java.util.Scanner;

import test.MessageNotFoundException;
import utilities.Address;
import utilities.FileContent;
import utilities.Pair;
import utilities.WriteMsgResponse;

public class Client {
	private final MasterServerClientInterface masterServer;

	public Client(Address mainServerAddress) throws RemoteException,
			NotBoundException {
		this.masterServer = (MasterServerClientInterface) getRemoteObject(mainServerAddress);
	}

	public void run() throws FileNotFoundException, RemoteException,
			IOException, NotBoundException, MessageNotFoundException {
		try {
			Map<String, ArrayList<FileContent>> filesData = new HashMap<String, ArrayList<FileContent>>();
			Map<String, Pair<ReplicaServerClientInterface, Integer>> transictionInfo = new HashMap<String, Pair<ReplicaServerClientInterface, Integer>>();

			Scanner in = new Scanner(System.in);
			System.out
					.println("Enter 0 for read,\n\t 1 for write,\n\t 2 for commit ,\n\t 3 for abort;\n\t anything else to terminate!");
			while (in.hasNext()) {
				int type = in.nextInt();
				if (type == 0) {
					System.out.println("enter file name to read:");
					String fileName = in.next();

					try {
						FileContent file = read(fileName);
						System.out
								.println("\nCLIENT : Read File : " + fileName);
						System.out.println(file.toString());
					} catch (FileNotFoundException e) {
						System.err.println("File " + fileName
								+ " doesn't exist in the system!");
					}

				} else if (type == 1) {
					System.out
							.println("Enter 0 for read,\n\t 1 for write,\n\t 2 for commit ,\n\t 3 for abort;\n\t anything else to terminate!");
					String fileName = in.next();
					ArrayList<FileContent> data = filesData.get(fileName);
					if (data == null) {
						data = new ArrayList<FileContent>();
					}
					String nextLine;
					while (!(nextLine = in.nextLine()).equalsIgnoreCase("-1")) {
						data.add(new FileContent(fileName, nextLine + '\n',
								true));
					}
					System.out.println("CLIENT : Writing File " + fileName);

					Pair<ReplicaServerClientInterface, Integer> value = write(
							fileName, data);
					transictionInfo.put(fileName, value);

					filesData.put(fileName, data);
				} else if (type == 2) {
					System.out.println("Enter fileName to commit");
					String fileName = in.next();
					Pair<ReplicaServerClientInterface, Integer> value = transictionInfo
							.get(fileName);
					commit(fileName, value.first(), value.second(), filesData
							.get(fileName).size());
					filesData.remove(fileName);
					transictionInfo.remove(fileName);
				} else if (type == 3) {
					System.out.println("Enter fileName to abort");
					String fileName = in.next();
					Pair<ReplicaServerClientInterface, Integer> value = transictionInfo
							.get(fileName);
					value.first().abort(value.second());
					filesData.remove(fileName);
					transictionInfo.remove(fileName);
				} else {
					System.err.println("Terminating");
					in.close();
					return;
				}
				System.out
						.println("Enter 0 for read,\n\t 1 for write,\n\t 2 for commit ,\n\t 3 for abort;\n\t anything else to terminate!");
			}
			in.close();
			System.out.println("Client is out!");
		} catch (Exception e) {
			e.printStackTrace();
		}
		/*
		 * String fileName = "hello.txt";
		 * System.out.println("\nTest Writing File : " + fileName);
		 * ArrayList<FileContent> data = new ArrayList<FileContent>(); for (int
		 * i = 0; i < 5; i++) { data.add(new FileContent(fileName, "Write " + i
		 * + "\n", true)); }
		 * 
		 * write(fileName, data);
		 * 
		 * FileContent file = read(fileName);
		 * System.out.println("\nCLIENT : Read File : " + fileName);
		 * System.out.println(file.toString());
		 */}

	public FileContent read(String fileName) throws FileNotFoundException,
			RemoteException, IOException, NotBoundException {
		// get the address of primary replicated server
		Address primaryReplicaAddress = masterServer.read(fileName);

		// get remote primary replica object
		ReplicaServerClientInterface primaryReplica = (ReplicaServerClientInterface) getRemoteObject(primaryReplicaAddress);

		// read file
		FileContent file = primaryReplica.read(fileName);
		return file;
	}

	public Pair<ReplicaServerClientInterface, Integer> write(String fileName,
			ArrayList<FileContent> data) throws FileNotFoundException,
			RemoteException, IOException, NotBoundException,
			MessageNotFoundException {
		System.out.println("CLIENT : Writing File : " + fileName);

		System.out.println("CLIENT : Send Request to Master Server");
		// get the address of primary replicated server
		WriteMsgResponse masterResponse = masterServer.write(fileName);

		System.out.println("CLIENT : Get the Primary Replica : "
				+ masterResponse.loc.toString());

		// get remote primary replica object
		ReplicaServerClientInterface primaryReplica = (ReplicaServerClientInterface) getRemoteObject(masterResponse.loc);

		for (int i = 0; i < data.size(); i++) {
			System.out
					.println("\nCLIENT : Send Write Requst to PrimaryReplica : transactionID : "
							+ masterResponse.transactionId
							+ ", SequenceNum : "
							+ i);
			primaryReplica.write(masterResponse.transactionId, i, data.get(i));
		}
		return new Pair<ReplicaServerClientInterface, Integer>(primaryReplica,
				masterResponse.transactionId);
		// System.out
		// .println("\nCLIENT : Send Commit Request to Primary Replica : transactionID : "
		// + masterResponse.transactionId);
		// boolean finishedSuccessfully = primaryReplica.commit(
		// masterResponse.transactionId, data.size());
		// System.out.println("\nCLIENT : Commit Response : "
		// + finishedSuccessfully);

	}

	private void commit(String fileName,
			ReplicaServerClientInterface primaryReplica, int transactionId,
			int dataSize) throws RemoteException, IOException,
			NotBoundException, MessageNotFoundException {
		System.out
				.println("\nCLIENT : Send Commit Request to Primary Replica : transactionID : "
						+ transactionId);
		boolean finishedSuccessfully = primaryReplica.commit(transactionId,
				dataSize);
		System.out.println("\nCLIENT : Commit Response : "
				+ finishedSuccessfully);
	}

	private Remote getRemoteObject(Address serverAddr) throws RemoteException,
			NotBoundException {
		// get registry on the serverAdress
		Registry registry = LocateRegistry.getRegistry(serverAddr.ipAddr,
				serverAddr.portNumber);

		// currently we use the same reference, try to process it every time
		return registry.lookup(serverAddr.objectName);
	}

	public static void main(String[] args) throws IOException,
			NotBoundException, MessageNotFoundException {
		BufferedReader br1 = new BufferedReader(
				new FileReader("masterAddr.txt"));
		String[] tokens1 = br1.readLine().split(" ");
		br1.close();

		Address masterAddress = new Address(tokens1[0],
				Integer.parseInt(tokens1[1]), tokens1[2]);

		// start clients
		Client client = new Client(masterAddress);
		client.run();
		System.out.println("THE END!");
		System.exit(0);

	}

}
