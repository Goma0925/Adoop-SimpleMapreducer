package ao.adoop.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;

import ao.adoop.mapreduce.Configuration;

public class FileSystemManager {
	//This class creates, deletes, validates, and organize necessary files and directories for the program.
	
	private Configuration config = null;

	public FileSystemManager(Configuration config) {
		this.config = config;
	}

	public void initFileSystem() throws IOException {		
		Path[] dirPaths = {
				this.config.systemBaseDir,
				this.config.mapOutputBufferDir,
				this.config.reduceOutputBufferDir,
				this.config.inputDir,
				this.config.jobConfigDir,
				this.config.namedReduceOutputBufferDir
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
	
	public void clearMapOutputBufferDir() throws NotDirectoryException {
		//Delete all the files and directories in the map output base dir.
		File mapOutputBufferDir = this.config.mapOutputBufferDir.toFile();
		if (!mapOutputBufferDir.isDirectory()) {
			throw new NotDirectoryException("MapOutputBufferDir '" + mapOutputBufferDir.toString() + "' is not a directory.");
		};

		File[] mapOutputDirs = mapOutputBufferDir.listFiles();
		for (File mapOutputDir: mapOutputDirs) {
        	recursiveDelete(mapOutputDir);
        };
	};
	
	public void clearReduceOutputBufferDir() throws NotDirectoryException {
		//Delete all the files and directories in the map output base dir.
		File reduceOutputBaseDir = this.config.reduceOutputBufferDir.toFile();
		if (!reduceOutputBaseDir.isDirectory()) {
			throw new NotDirectoryException("ReduceOutputBufferDir '" + reduceOutputBaseDir.toString() + "' is not a directory.");
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
		for (File f: results) {
			System.out.println("FileSystem-reduceOutputFile:" + f.toString());
		}
		return results;
	}
	
	public void mergeReduceOutputs(File reduceOutputBufferDir, File finalOutputFile) throws IOException {
		//Merge multiple reduce files in the reduceOutputBuffer directory to a single output file
		//specified as finalOutputFile.
		if (!reduceOutputBufferDir.isDirectory()) {
			new NotDirectoryException("The reduceOutputBufferDir '" + reduceOutputBufferDir.toString() + "' is not a directory");
		};
		ArrayList<File> reduceOutputFiles = this.getAllChildFiles(reduceOutputBufferDir);
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
		System.out.println("Finished merging output files to :" + finalOutputFile.toString());
	}
}
