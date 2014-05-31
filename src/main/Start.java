package main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.util.Scanner;

import rmiObjects.RmiMasterServer;
import rmiObjects.RmiReplicaServer;
import test.MessageNotFoundException;
import utilities.Address;

public class Start {

	public static void startNewInstance(Class<?> klass, String token0, String token1, String token2)
			throws IOException, InterruptedException {
		String javaHome = System.getProperty("java.home");
		String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
		String classpath = System.getProperty("java.class.path");
		String className = klass.getCanonicalName();

		ProcessBuilder builder = new ProcessBuilder(javaBin, "-cp", classpath, className, token0,
				token1, token2);
		Process process = builder.start();
		// process.waitFor();
		System.out.println("================ ");
		// Scanner in = new Scanner(process.getInputStream());
		// while(in.hasNext()){
		// System.out.println(in.nextLine());
		// }
		System.out.println("Starting Master Server");
		System.out.println("================ ");
		// System.out.println(process.exitValue());
	}

	public static void startNewInstance(Class<?> klass, String token0, String token1,
			String token2, String token3, String token4, String token5, String token6)
			throws IOException, InterruptedException {
		String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator
				+ "java";
		String classpath = System.getProperty("java.class.path");
		String className = klass.getCanonicalName();

		ProcessBuilder builder = new ProcessBuilder(javaBin, "-cp", classpath, className, token0,
				token1, token2, token3);
		Process process = builder.start();
		// process.waitFor();
		System.out.println("================ ");
		System.out.println("Starting replicate server number " + token6);
		// Scanner in = new Scanner(process.getInputStream());
		// while(in.hasNext()){
		// System.out.println(in.nextLine());
		// }
		System.out.println("================ ");
		// System.out.println(process.exitValue());
	}

	// String clientType, String clientAddress, String serverAddress,
	// int portNumber, int numberOfAcesses, String objectName)
	// throws IOException, InterruptedException {
	//
	// String javaBin = System.getProperty("java.home") + File.separator
	// + "bin" + File.separator + "java";
	// String classpath = System.getProperty("java.class.path");
	// String className = klass.getCanonicalName();
	//
	// ProcessBuilder builder = new ProcessBuilder(javaBin, "-cp", classpath,
	// className, Integer.toString(clientID), clientType,
	// clientAddress, serverAddress, Integer.toString(portNumber),
	// Integer.toString(numberOfAcesses), objectName);
	// Process process = builder.start();
	// }

	static void newProcessesMain(String[] tokens1) throws IOException, NotBoundException,
			MessageNotFoundException, InterruptedException {

		startNewInstance(RmiMasterServer.class, tokens1[0], tokens1[1], tokens1[2]);
		System.out.println("================");
		// start replicated servers
		BufferedReader br2 = new BufferedReader(new FileReader("replicated.txt"));
		int n = Integer.parseInt(br2.readLine());
		for (int i = 0; i < n; i++) {
			String[] tokens2 = br2.readLine().split(" ");
			startNewInstance(RmiReplicaServer.class, tokens2[0], tokens2[1], tokens2[2],
					tokens1[0], tokens1[1], tokens1[2], i + "");
		}
		br2.close();

		Thread.sleep(100);
	}

	static void regularMain(Address masterAddress) throws IOException, NotBoundException,
			MessageNotFoundException, InterruptedException {
		// start master server
		new RmiMasterServer(masterAddress, "ServersData/Master/");

		// start replicated servers
		BufferedReader br2 = new BufferedReader(new FileReader("replicated.txt"));
		int n = Integer.parseInt(br2.readLine());
		for (int i = 0; i < n; i++) {
			String[] tokens2 = br2.readLine().split(" ");
			new RmiReplicaServer(new Address(tokens2[0], Integer.parseInt(tokens2[1]), tokens2[2]), masterAddress,
					"ServersData/Replica" + i + "/");
		}
		br2.close();
		Thread.sleep(100);
		Client c = new Client(masterAddress);
		c.run();
	}

	public static void main(String[] args) throws IOException, NotBoundException,
			MessageNotFoundException, InterruptedException {
		BufferedReader br1 = new BufferedReader(new FileReader("masterAddr.txt"));
		String[] tokens1 = br1.readLine().split(" ");
		br1.close();

		Address masterAddress = new Address(tokens1[0], Integer.parseInt(tokens1[1]), tokens1[2]);

		 regularMain(masterAddress);
//		newProcessesMain(tokens1);
	}
}
