package ao.adoop.test.utils.ui;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import ao.adoop.ui.CommandHandler;
import ao.adoop.ui.UnknownOptionException;
import javafx.util.Pair;

class CommandArgParserTest {

	@Test
	void test() throws UnknownOptionException {
		String[] validOptions = {"-input", "-jobcon"};
		
		//CASE: A valid command with two option flags containing several arguments. 
		String[] args1 = {"mode-command", "-input", "input1.txt", "input2.txt", "-jobcon", "jobConfig.java"};
		int[] optionFlagStartIndices = {1, 4};
		checkParsedValues(args1, validOptions, optionFlagStartIndices);
		
		//CASE: Command contains sandomValue without option flag
		String[] args2 = {"mode-command1", "someRandomValue", "-input", "input1.txt", "input2.txt", "input3.txt", 
					"-jobcon", "jobConfig.java"};
		int[] optionFlagStartIndices2 = {2, 6};
		checkParsedValues(args2, validOptions, optionFlagStartIndices2);
		
		//CASE: Command contains an invalid option flag.
		String[] args3 = {"mode-command1", "someRandomValue", "-input", "input1.txt", "input2.txt", "input3.txt", 
				"-invalid", "jobConfig.java"};
		int[] optionFlagStartIndices3 = {2, 6};
		checkParsedValues(args3, validOptions, optionFlagStartIndices3);
	};
	
	public static void checkParsedValues(String[] args, String[] validOptions, int[] optionFlagStartIndices) throws UnknownOptionException {
		printOriginalInput(args);
		CommandHandler cHandler = null;
		if (!allOptionsAreValid(args, validOptions)){
			try {
				cHandler = new CommandHandler(args, validOptions);
				fail("CommandHandler failed to throw an Exception with (an) invalid option(s)\n"
						+ "		ORGINAL_ARGS: " + Arrays.asList(args).toString() + "/" + "VALID_OPTIONS:" + Arrays.asList(validOptions).toString());
			}catch (UnknownOptionException e) {
				System.out.println("	Successfully caught invalid options with UnknownOptionException");
			}
				
		}else {
			cHandler = new CommandHandler(args, validOptions);
			//Check parsed command value
			String originalCommand = args[0];
			String parsedCommand = cHandler.getModeCommand();
			Assertions.assertEquals(originalCommand, parsedCommand);
			
			//Check parsed option values.
			String originalOptionFlag = null;
			String[] originalOptionArgs = null;
			int optionFlagStartIndex;
			for (int i=0; i<optionFlagStartIndices.length; i++) {
				optionFlagStartIndex = optionFlagStartIndices[i]; //The index where the option flag starts
				originalOptionFlag = args[optionFlagStartIndex]; //Option flag like "-input"
				System.out.println("	Checking if the OPTION_FLAG exists in the parsed value: '"+ originalOptionFlag + "'");
				Assertions.assertEquals(cHandler.containsOptionFlag(originalOptionFlag), true);
				if (i == optionFlagStartIndices.length - 1) {
					//If we are at the last optionFlagStartIndex, extract the array starting from the optionFlagStartIndex
					//to the end of the original args.
					originalOptionArgs = getSliceOfArray(args, optionFlagStartIndices[i]+1, args.length);
				}else {
					originalOptionArgs = getSliceOfArray(args, optionFlagStartIndices[i]+1, optionFlagStartIndices[i+1]);
				};
				for (int j=0; j<originalOptionArgs.length; j++) {
					//Check if the parsed result correctly extracted the original option arguments.
					System.out.println("		"+Integer.toString(j)+"th argument: "+"ORIGINAL_INPUT="+args[(optionFlagStartIndex+1)+j] + " | PARSED_VALUE=" + cHandler.getOptionArgs(originalOptionFlag).get(j));
					Assertions.assertEquals(args[(optionFlagStartIndex+1)+j], cHandler.getOptionArgs(originalOptionFlag).get(j));
				}
			}
		}
	}
	
	private static boolean allOptionsAreValid(String[] args, String[] validOptions) {
		List<String> validOptionList = Arrays.asList(validOptions);
		for (int i=0; i<args.length; i++) {
			if (args[i].charAt(0) == '-' && !validOptionList.contains(args[i])) {
				return false;
			}
		}
		return true;
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
		System.out.print("\nATTEMPT TO PARSE: '");
		for (String arg: args) {
			System.out.print(arg+" ");
		}
		System.out.println("'");
		}
}
