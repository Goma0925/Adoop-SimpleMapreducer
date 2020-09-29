package ao.adoop.mapreduce;

import java.nio.file.InvalidPathException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Configuration {
	private Path baseDir = Paths.get("");
	
	private Path mapOutputBufferDir = Paths.get(baseDir.toString(), "map-output-buffer");
	private String mapOutputFileExtension = ".csv";
	
	private Path finalOutputDir = null; //To be set by user.
	private String finalOutputFileExtension = ".csv";
	
	private long threadMaxThreashold = 30;
	private String threadMaxThreasholdUnit = "MB";
	private int maxThreadNum = Runtime.getRuntime().availableProcessors();
	
	public String getMapOutputFileName(String workerId) {
		return "part-m-" + workerId + this.mapOutputFileExtension.toString();
	}
		
	public Path getBaseDir() {
		return baseDir;
	}

	public void setBaseDir(Path baseDir) {
		this.baseDir = baseDir;
	}

	public Path getMapOutputBufferDir() {
		return mapOutputBufferDir;
	}

	public void setMapOutputBufferDir(Path mapOutputBufferDir) {
		this.mapOutputBufferDir = mapOutputBufferDir;
	}

	public String getMapOutputFileExtension() {
		return mapOutputFileExtension;
	}

	public void setMapOutputFileExtension(String mapOutputFileExtension) {
		this.mapOutputFileExtension = mapOutputFileExtension;
	}

	public Path getFinalOutputDir() {
		return finalOutputDir;
	}

	public void setFinalOutputDir(Path finalOutputDir) throws NotDirectoryException {
		this.finalOutputDir = finalOutputDir;	
	}

	public String getFinalOutputFileExtension() {
		return finalOutputFileExtension;
	}

	public void setFinalOutputFileExtension(String finalOutputFileExtension) {
		this.finalOutputFileExtension = finalOutputFileExtension;
	}

	public long getThreadMaxThreashhold() {
		return threadMaxThreashold;
	}

	public void setThreadMaxThreashold(long threadMaxThreashhold) {
		this.threadMaxThreashold = threadMaxThreashhold;
	}

	public String getThreadMaxThreasholdUnit() {
		return threadMaxThreasholdUnit;
	}

	public void setThreadMaxThreashholdUnit(String threadMaxThreashholdUnit) {
		this.threadMaxThreasholdUnit = threadMaxThreashholdUnit;
	}

	public String generateReduceOutputFileName(String workerId) {
		return "part-r-" + workerId + this.finalOutputFileExtension;
	};

	public void check() {
		if (this.finalOutputDir == null) {
			throw new InvalidPathException(this.finalOutputFileExtension, "The final output path cannot be null.");
		}
	};
	
	public void setMaxThreadNum(int maxThreadNum) {
		this.maxThreadNum = maxThreadNum;
	};
	
	public int getMaxThreadNum() {
		return this.maxThreadNum;
	}
}
