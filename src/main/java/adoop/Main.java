package adoop;

import java.io.IOException;
import java.nio.file.Files;

import filehandler.FileSystemManager;
import settings.SystemPathSettings;


public class Main {	
	public static void main(String[] args) throws Exception {
		setup();
//		JobScheduler master = new JobScheduler();
//		master.start(args);
		
	};
	
	public static void setup() throws IOException {
		FileSystemManager.clearMapOutputDir();
	}
}
