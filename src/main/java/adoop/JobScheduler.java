package adoop;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class JobScheduler {
	private Timer timer = new Timer();
	private UserInterface userInterface = new CommandLineInterface();
	private ArrayList<File> reducerInputDirs = new ArrayList<File>();
	private Configuration jobConfig = null;
	
	public void start(String[] args) throws Exception {
	};


	public void runMap(Class<?> mapperClass, File inputFile) {
		 
	};
	
	private void saveReducerInputPaths(File[] mapOutputPaths) {
		
	};
	
	private ArrayList<File> loadReducerInputDirs() {
		return reducerInputDirs;
	};
	
	public void runReduce(Class<?> reducerClass) {
		
	}
}
