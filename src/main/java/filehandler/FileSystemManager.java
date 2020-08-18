package filehandler;

import java.io.File;
import java.io.IOException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;

import settings.SystemPathSettings;

public class FileSystemManager {
	//This class creates, deletes, and validates necessary files and directories for the program.
	
	public static void initFileSystem() throws IOException {		
		Path[] dirPaths = {
				SystemPathSettings.systemBaseDir,
				SystemPathSettings.mapOutputBaseDir,
				SystemPathSettings.reduceOutputBaseDir,
				SystemPathSettings.inputDir,
				SystemPathSettings.jobConfigDir
		};
		System.out.println("Checking and setting up the required directories...");
		for (Path dirPath: dirPaths) {
			createDir(dirPath);
		}
	};
	
	private static void createDir(Path dirPath) {
		File dir= dirPath.toFile();
		if (!dir.exists()) {
			dir.mkdir();
			System.out.println("	Created a new directory: '"+ dir.toString()+"'");
		}
	};
	
	private static void recursiveDelete(File file) {
		File[] children = file.listFiles();
		if (children != null) {
			for (File child: children) {
				recursiveDelete(child);
			}
		};
		file.delete();
	}
	
	public static void clearMapOutputDir() throws NotDirectoryException {
    	System.out.println("Clear");
		//Delete all the files and directories in the map output base dir.
		File mapOutputBaseDir = SystemPathSettings.mapOutputBaseDir.toFile();
		if (!mapOutputBaseDir.isDirectory()) {
			throw new NotDirectoryException("MapOutputBaseDir '" + mapOutputBaseDir.toString() + "' is not a directory.");
		};

		File[] mapOutputDirs = mapOutputBaseDir.listFiles();
		for (File mapOutputDir: mapOutputDirs) {
        	System.out.println("INITIAL:"+mapOutputDir.toString());
        	recursiveDelete(mapOutputDir);
        };
	}
}
