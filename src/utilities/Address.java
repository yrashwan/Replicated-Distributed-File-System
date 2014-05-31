package utilities;

import java.io.Serializable;

public class Address implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4155677321648825510L;
	public String ipAddr;
	public int portNumber;
	public String objectName;

	public Address(String ipAddr, int portNum, String name) {
		this.ipAddr = ipAddr;
		this.portNumber = portNum;
		this.objectName = name;
	}

	@Override
	public int hashCode() {
		return objectName.hashCode();
	}
	
	public boolean equals(Address addr) {
		return objectName.equals(addr.objectName);
	}
	@Override
	public String toString() {
		return  ipAddr + "<" + portNumber + "<" + objectName;
	}
}
