package adoop;

import java.io.IOException;
import java.nio.file.Files;


public class Main {	
	public static void main(String[] args) throws Exception {
		setup();
		JobScheduler master = new JobScheduler();
		master.start(args);
	};
	
	public static void setup() throws IOException {
		System.out.println("Setting up the required directories...");
		if (!Constants.systemBaseDir.toFile().exists()) {
			Files.createDirectories(Constants.systemBaseDir);
			System.out.println("	Created Settings.systemBaseDir: '"+Constants.systemBaseDir.toString()+"'");
		};
		if (!Constants.mapOutputBaseDir.toFile().exists()) {
			Files.createDirectories(Constants.mapOutputBaseDir);
			System.out.println("	Created Settings.mapOutputBaseDir: '"+Constants.mapOutputBaseDir.toString()+"'");
		};
		if (!Constants.reduceOutputBaseDir.toFile().exists()) {
			Files.createDirectories(Constants.reduceOutputBaseDir);
			System.out.println("	Created Settings.reduceOutputBaseDir: '"+Constants.reduceOutputBaseDir.toString()+"'");
		}
		if (!Constants.inputDir.toFile().exists()) {
			Files.createDirectories(Constants.inputDir);
			System.out.println("	Created Settings.inputDir: '"+Constants.inputDir.toString()+"'");
		}
		if (!Constants.jobConfigDir.toFile().exists()) {
			Files.createDirectories(Constants.jobConfigDir);
			System.out.println("	Created Settings.jobConfigDir: '"+Constants.jobConfigDir.toString()+"'");
		}
	}
}
