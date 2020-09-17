package ao.adoop.test.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Assertions;

public class CustomAssertions {
	public static void assertEachFileContent(Path answerDir, Path outputDir) throws IOException {
		//Assert if each file in the answerDir and outputDir have the exact same contents
		File[] answerFiles = SimpleFileLoader.getChildFiles(answerDir);
		File[] outputFiles = SimpleFileLoader.getChildFiles(outputDir);
		System.out.println("Checking output files agaist answer files...");
		System.out.println("	AnswerDir: " + answerDir.toString());
		System.out.println("	outputDir: " + outputDir.toString());
		for (int i=0; i<outputFiles.length; i++) {
			System.out.println("	Checking if file contents match:");
			System.out.println("	  - Answer file("+ Integer.toString(i) +"): " + answerFiles[i].toString());
			System.out.println("	  - Output file:"+ Integer.toString(i) +"): " + outputFiles[i].toString());
			String answerLines[] = SimpleFileLoader.readFile(answerFiles[i]);
			String outputLines[] = SimpleFileLoader.readFile(outputFiles[i]);
			try {
				Assertions.assertArrayEquals(answerLines, outputLines);				
			}catch (Exception e) {
				for (int j=0; j<outputLines.length; j++) {
					System.out.println("		ANSWER LINE("+Integer.toString(i)+"):" + answerLines[i]);
					System.out.println("		OUTPUT LINE("+Integer.toString(i)+"):" + outputLines[i]);
				};
			};
		};
	};
}
