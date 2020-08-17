package commandparser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javafx.util.Pair;

public class CommandHandler {
	String commandModeName = null;
	Map<String, ArrayList<String>> optionMap = null;
	
	public CommandHandler(String[] args, String[] validOptions) throws UnknownOptionException {
		Pair<String, Map<String, ArrayList<String>>> commandBox = this.parseArgs(args, validOptions);
		this.commandModeName = commandBox.getKey();
		Map<String, ArrayList<String>> optionMapTempo =  commandBox.getValue();
		this.checkForInvalidOption(validOptions, optionMapTempo);
		this.optionMap = optionMapTempo;
	};

	private void checkForInvalidOption(String[] validOptions, Map<String, ArrayList<String>> optionMap) throws UnknownOptionException {
		List<String> validOptionList = Arrays.asList(validOptions);
		for (String option: optionMap.keySet()) {
			if (!validOptionList.contains(option)) {
				throw new UnknownOptionException(option);
			}
		}
	}

	private Pair<String, Map<String, ArrayList<String>>> parseArgs(
			String[] args, String[] validOptions) {
		//String[] validOptions: An array of valid option strings. If the args contain an option that 
		// 							is not in this array, an error will be raised.
		String command = args[0];
		Map<String, ArrayList<String>> options = new HashMap<String, ArrayList<String>>();
		ArrayList<String> buffer = new ArrayList<String>();
		String currentOption = null;
		for (int i=1; i<args.length; i++) {
			if (args[i].charAt(0) == '-') {
				buffer.clear();
				currentOption = args[i];
			}else{
				if (currentOption != null) {
					if (options.get(currentOption) == null) {
						options.put(currentOption, new ArrayList<String>());
					};
					options.get(currentOption).add(args[i]);
				};
			};
		};
		return new Pair<String, Map<String, ArrayList<String>>>(command, options);
	};
	
	public boolean containsOptionFlag(String optionFlag) {
		return this.optionMap.containsKey(optionFlag);
	};
	
	public String getModeCommand() {
		return this.commandModeName;
	};
	
	public ArrayList<String> getOptionArgs(String optionFlag){
		return this.optionMap.get(optionFlag);
	};
}
