package utilities;

import java.io.Serializable;

public class FileContent implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4000588711148820835L;
	public String fileName;
	public boolean isPrimary;
	public String data;
	
	public FileContent(FileContent content) {
		fileName = content.fileName;
		isPrimary = content.isPrimary;
		data = content.data;
	}
	
	public FileContent(String fileName, String data, boolean isPrimary)
	{
		this.fileName = fileName;
		this.data = data;
		this.isPrimary = isPrimary;
	}
	
	@Override
	public String toString() {
		return data;
	}
}
