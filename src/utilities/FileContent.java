package utilities;

public class FileContent {
	public String fileName;
	public boolean isPrimary;
	public String data;
	
	public FileContent(FileContent content) {
		fileName = content.fileName;
		isPrimary = content.isPrimary;
		data = content.data;
	}
}
