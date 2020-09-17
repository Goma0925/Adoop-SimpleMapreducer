package ao.adoop.test.unit;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import ao.adoop.io.DataLoader;

public class DataLoaderTest {
	DataLoader loader = new DataLoader();
	File inputFile = new File("src/test/resources/loader-input-files/ascending-to-100000.csv");
	
	@Test
	void countLines() throws IOException {
		int lineCount = this.loader.countLines(this.inputFile);
		Assertions.assertEquals(100000, lineCount);
	}
	
	@Test 
	void getChunkIndices() throws Exception{
		//Get an array list specifying the approximate divisions
		// that separate a file in chunks of a certain size
		// e.g) When specifying 100KB for each chunk, the chunkIndices array list will contain
		// 		a pair of indices where each chunk of size 100KB starts and ends approximately.
		 long maxChunkSize = 100; 
		 String unit = "KB";
		 ArrayList<int[]> chunkIndices = this.loader.getChunkIndices(this.inputFile, maxChunkSize, unit);
		 //Assert if the number of chunks is 6 because the inputFile is almost 600KB and we are splitting it 
		 //into 100KB chunks.
		 Assertions.assertEquals(chunkIndices.size(), 6);
	}
	
	@Test
	void loadChunkByLineIndices() throws IOException {
		//Read the lines between particular indices (300 to 1000th lines) from a file
		int start = 10;
		int end = 50;
		String[] lines = this.loader.loadChunkByLineIndices(this.inputFile, start, end);
		String[] answerLines = new String[end-start];
		for (int index=start; index<end; index++) {
			answerLines[index-start] = Integer.toString(index);
		};
		Assertions.assertArrayEquals(answerLines, lines);
	}
	
	@Test
	void loadFile() throws IOException {
		ArrayList<String> lines = this.loader.loadFile(this.inputFile);
		ArrayList<String> answerLines = new ArrayList<String>();
		for (int i=0; i<100000; i++) {
			answerLines.add(Integer.toString(i));
		}
		Assertions.assertArrayEquals(lines.toArray(), answerLines.toArray());
	}
}
