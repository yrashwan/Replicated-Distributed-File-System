package utilities;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.HashMap;

public class MethodsUtility {
	public static Remote getRemoteObject(Address serverAddr) throws RemoteException,
			NotBoundException {
		// get registry on the serverAdress
		Registry registry = LocateRegistry.getRegistry(serverAddr.ipAddr, serverAddr.portNumber);

		// currently we use the same reference, try to process it every time
		return registry.lookup(serverAddr.objectName);
	}

	public static String readFromDisk(String fileDir) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fileDir));
			String s = "";
			while (br.ready()) {
				s += br.readLine() + "\n";
			}
			br.close();
			return s;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static void writeToDisk(String fileDir, String data) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(fileDir));
			bw.write(data);
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void appendToFileOnDisk(String fileDir, String data) {
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(fileDir, true));
			bw.write(data);
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String convertToString(Address[] addr) {
		String res = addr[0].toString();
		for (int i = 1; i < addr.length; i++)
			res += " " + addr[i].toString();
		return res;
	}

	public static boolean existsOnDisk(String fileDir) {
		File file = new File(fileDir);
		return file.exists();
	}

	public static HashMap<String, Address[]> readMetaData(String fileDir) {
		HashMap<String, Address[]> map = new HashMap<String, Address[]>();
		if (MethodsUtility.existsOnDisk(fileDir)) {
			String metaFile = MethodsUtility.readFromDisk(fileDir);
			String[] files = metaFile.split("\n");
			for (int i = 0; i < files.length; i++)
				if (files[i].length() > 0) {
					String[] replicas = files[i].split(" ");
					// first token file name
					String fileName = replicas[0];

					// then the addresses to replicas
					System.out.println(replicas.length);
					Address[] address = new Address[replicas.length - 1];
					for (int j = 1; j < replicas.length; j++) {
						String[] tokens = replicas[j].split("<");
						System.out.println(replicas[j]);
						System.out.println(Arrays.toString(tokens));
						address[j - 1] = new Address(tokens[0], new Integer(tokens[1]), tokens[2]);
					}

					map.put(fileName, address);
				}
		} else {
			return new HashMap<String, Address[]>();
		}
		return map;
	}

	public static void writeMetaData(String fileDir, HashMap<String, Address[]> map) {
		String out = "";
		for (String key : map.keySet()) {
			out += key + " " + MethodsUtility.convertToString(map.get(key)) + "\n";
		}
		MethodsUtility.writeToDisk(fileDir, out);
	}
}
