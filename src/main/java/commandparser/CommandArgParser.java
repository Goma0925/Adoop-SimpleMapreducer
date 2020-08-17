package commandparser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javafx.util.Pair;

public class CommandArgParser {
	public static Pair<String, Map<String, ArrayList<String>>> parseArgs(String[] args) {
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
}
