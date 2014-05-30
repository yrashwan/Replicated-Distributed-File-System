package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.NotBoundException;

import rmiObjects.RmiMasterServer;
import rmiObjects.RmiReplicaServer;
import test.MessageNotFoundException;
import utilities.Address;

public class Start {
	public static void main(String[] args) throws IOException, NotBoundException, MessageNotFoundException, InterruptedException {
		BufferedReader br1 = new BufferedReader(new FileReader("masterAddr.txt"));
		String[] tokens1 = br1.readLine().split(" ");
		br1.close();
		
		// start master server
		Address masterAddress = new Address(tokens1[0], Integer.parseInt(tokens1[1]), tokens1[2]);
		new RmiMasterServer(masterAddress, "ServersData/Master/");

		// start replicated servers
		BufferedReader br2 = new BufferedReader(new FileReader("replicated.txt"));
		int n = Integer.parseInt(br2.readLine());
		for (int i = 0; i < n; i++) {
			String[] tokens2 = br2.readLine().split(" ");
			new RmiReplicaServer(new Address(tokens2[0], Integer.parseInt(tokens2[1]), tokens2[2]), "ServersData/Replica" + i + "/");
		}
		br2.close();
		
		Thread.sleep(100);
		
		// start clinets
		Client client = new Client(masterAddress);
		client.run();
	}
}
