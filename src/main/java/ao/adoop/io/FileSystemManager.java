package ao.adoop.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;

import ao.adoop.settings.SystemPathSettings;

public class FileSystemManager {
	//This class creates, deletes, validates, and organize necessary files and directories for the program.
	
	private SystemPathSettings pathSettings = null;

	public FileSystemManager(SystemPathSettings pathSettings) {
		this.pathSettings = pathSettings;
	}

	public void initFileSystem() throws IOException {		
		Path[] dirPaths = {
				this.pathSettings.systemBaseDir,
				this.pathSettings.mapOutputBaseDir,
				this.pathSettings.reduceOutputBaseDir,
				this.pathSettings.inputDir,
				this.pathSettings.jobConfigDir,
				this.pathSettings.finalOutputDir
		};
		System.out.println("Checking and setting up the required directories...");
		for (Path dirPath: dirPaths) {
			this.createDir(dirPath);
		}
	};
	
	private void createDir(Path dirPath) {
		File dir= dirPath.toFile();
		if (!dir.exists()) {
			dir.mkdirs();
			System.out.println("	Created a new directory: '"+ dir.getAbsolutePath()+"'");
			System.out.println(new File(dir.getAbsolutePath()).exists());
		}else {
			System.out.println("	Required directory already exists: '"+ dir.getAbsolutePath()+"'");
		}
	};
	
	private void recursiveDelete(File file) {
		File[] children = file.listFiles();
		if (children != null) {
			for (File child: children) {
				recursiveDelete(child);
			}
		};
		file.delete();
	};
	
	public void clearMapOutputDir() throws NotDirectoryException {
		//Delete all the files and directories in the map output base dir.
		File mapOutputBaseDir = this.pathSettings.mapOutputBaseDir.toFile();
		if (!mapOutputBaseDir.isDirectory()) {
			throw new NotDirectoryException("MapOutputBaseDir '" + mapOutputBaseDir.toString() + "' is not a directory.");
		};

		File[] mapOutputDirs = mapOutputBaseDir.listFiles();
		for (File mapOutputDir: mapOutputDirs) {
        	recursiveDelete(mapOutputDir);
        };
	};
	
	public void clearReduceOutputDir() throws NotDirectoryException {
		//Delete all the files and directories in the map output base dir.
		File reduceOutputBaseDir = this.pathSettings.reduceOutputBaseDir.toFile();
		if (!reduceOutputBaseDir.isDirectory()) {
			throw new NotDirectoryException("ReduceOutputBaseDir '" + reduceOutputBaseDir.toString() + "' is not a directory.");
		};

		File[] reduceOutputDirs = reduceOutputBaseDir.listFiles();
		for (File reduceOutputDir: reduceOutputDirs) {
        	recursiveDelete(reduceOutputDir);
        };
	};
	
	private ArrayList<File> getAllChildFiles(File file) {
		ArrayList<File> results  =  new ArrayList<File>();
		if (file.listFiles().length == 0) {
			return results;
		}
		File[] childFiles = file.listFiles();
		for (File childFile: childFiles) {
			if (childFile.isFile()) {
				results.add(childFile);
			}else {
				results.addAll(this.getAllChildFiles(childFile));
			}
		};
		results.sort(Comparator.comparing(File::toString));
		return results;
	}
	
	public void mergeReduceOutputs(File finalOutputFile) throws IOException {
		//Merge multiple reduce files in the reduceOutputBuffer directory to a single output file
		//specified as finalOutputFile.
		ArrayList<File> reduceOutputFiles = this.getAllChildFiles(this.pathSettings.reduceOutputBaseDir.toFile());
		BufferedReader bReader = null;
		PrintWriter pWriter = new PrintWriter(finalOutputFile);
		String currentLine = "";
		for (File reduceOutputFile: reduceOutputFiles) {
			bReader = new BufferedReader(new FileReader(reduceOutputFile));
			currentLine = bReader.readLine();
			while (currentLine != null) {
				pWriter.println(currentLine);
				currentLine = bReader.readLine();
			}
		};
		pWriter.flush();
		pWriter.close();
		System.out.println("Finished printing in:" + finalOutputFile.toString());
	}
}
