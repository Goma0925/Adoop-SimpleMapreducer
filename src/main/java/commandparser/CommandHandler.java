package commandparser;

import java.util.ArrayList;

public class CommandHandler {
	CommandArgParser parser = new CommandArgParser();
	String modeCommand = null;
	
	private boolean containsInvalidOptions() {
		return false;
	}
	
	public boolean hasOptionFlag(String optionFlag) {
		return false;
	};
	
	public String getModeCommand() {
		return modeCommand;
	};
	
	public ArrayList<String> getOptionArgs(String optionFlag){
		return null;
	};
}
