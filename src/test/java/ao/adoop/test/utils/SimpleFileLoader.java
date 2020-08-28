package ao.adoop.test.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

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
}
