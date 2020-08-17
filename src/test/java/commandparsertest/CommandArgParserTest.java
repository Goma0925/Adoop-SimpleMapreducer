package commandparsertest;

import java.util.ArrayList;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import commandparser.CommandArgParser;
import javafx.util.Pair;

class CommandArgParserTest {

	@Test
	void test() {
		String[] args1 = {"mode command", "-input", "input1.txt", "input2.txt", "-jobcon", "jobConfig.java"};
		int[] optionFlagStartIndices = {1, 4};
		checkParsedValues(args1, optionFlagStartIndices);
		
		String[] args2 = {"mode command", "someRandomValue", "-input", "input1.txt", "input2.txt", "input3.txt", 
					"-jobcon", "jobConfig.java"};
		int[] optionFlagStartIndices2 = {2, 6};
		checkParsedValues(args2, optionFlagStartIndices2);
		
		String[] args3 = {"only mode command", "someRandomValue", "someRandomValue",};
		int[] optionFlagStartIndices3 = {};
		checkParsedValues(args3, optionFlagStartIndices3);
	};
	
	public static void checkParsedValues(String[] args, int[] optionFlagStartIndices) {
		printOriginalInput(args);
		Pair<String, Map<String, ArrayList<String>>> commandBox = CommandArgParser.parseArgs(args);
		
		//Check parsed command value
		String originalCommand = args[0];
		String parsedCommand = commandBox.getKey();
		Assertions.assertEquals(originalCommand, parsedCommand);
		
		//Check parsed option values.
		Map<String, ArrayList<String>> parsedCommandOptions = commandBox.getValue();
		String originalOptionFlag = null;
		String[] originalOptionArgs = null;
		int optionFlagStartIndex;
		int optionArgStartIndex;
		for (int i=0; i<optionFlagStartIndices.length; i++) {
			optionFlagStartIndex = optionFlagStartIndices[i]; //The index where the option flag starts
			optionArgStartIndex = optionFlagStartIndex + 1; //The index where the option arguments start.
			originalOptionFlag = args[optionFlagStartIndex]; //Option flag like "-input"
			System.out.println(optionFlagStartIndex);
			System.out.println("Checking OPTION_FLAG exists in the parsed value: ORIGINAL_OPTION_FLAG="+ originalOptionFlag);
			Assertions.assertEquals(parsedCommandOptions.containsKey(originalOptionFlag), true);
			if (i == optionFlagStartIndices.length - 1) {
				//If we are at the last optionFlagStartIndex, extract the array starting from the optionFlagStartIndex
				//to the end of the original args.
				originalOptionArgs = getSliceOfArray(args, optionFlagStartIndices[i]+1, args.length);
			}else {
				originalOptionArgs = getSliceOfArray(args, optionFlagStartIndices[i]+1, optionFlagStartIndices[i+1]);
			};
			for (int j=0; j<originalOptionArgs.length; j++) {
				//Check if the parsed result correctly extracted the original option arguments.
				System.out.println("	"+Integer.toString(j)+"th argument: "+"ORIGINAL_INPUT="+args[(optionFlagStartIndex+1)+j] + " | PARSED_VALUE=" + parsedCommandOptions.get(originalOptionFlag).get(j));
				Assertions.assertEquals(args[(optionFlagStartIndex+1)+j], parsedCommandOptions.get(originalOptionFlag).get(j));
			}
		}
	}
	
	public static String[] getSliceOfArray(String[] arr, int start, int end) { 
		// Get the slice of the Array 
		String[] slice = new String[end - start]; 
		
		// Copy elements of arr to slice 
		for (int i = 0; i < slice.length-1; i++) { 
			slice[i] = arr[start + i]; 
		} 
		
		// return the slice 
		return slice; 
	}
	
	public static void printOriginalInput(String[] args) {
		System.out.print("\nTRYING TO PARSE: '");
		for (String arg: args) {
			System.out.print(arg+" ");
		}
		System.out.println("'");
		}
}
