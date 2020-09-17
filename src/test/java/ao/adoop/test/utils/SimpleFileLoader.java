package ao.adoop.test.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;

public class SimpleFileLoader {
	public static String[] readFile(File file) throws IOException {
		FileReader fReader = new FileReader(file);
		BufferedReader bReader = new BufferedReader(fReader);
		ArrayList<String> lines = new ArrayList<String>();
		String line;
		do {
			line = bReader.readLine();
			if (line != null) {
				lines.add(line);				
			}
		}while(line != null);
		bReader.close();
		
		//Create the result array
		return lines.toArray(new String[lines.size()]);
	}
	
	public static File[] getChildFiles(Path dir) throws IOException {
		
		if (!dir.toFile().isDirectory()) {
			throw new NotDirectoryException("'" + dir.toString() + "' is not a directory.");
		}
		ArrayList<File> results = new ArrayList<File>();
		File[] childFiles = dir.toFile().listFiles();
		for (File file: childFiles) {
			if (file.isFile()) {
				results.add(file);				
			};
		};
		results.sort(Comparator.comparing(File::toString));
		return results.toArray(new File[results.size()]);
	}
}
