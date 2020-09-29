package ao.adoop.mapreduce;

import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;

public class Job {
	protected Configuration config = null;
	protected String jobName = null;
	protected ArrayList<MapTask> mapTasks = new ArrayList<MapTask>();
	private Class<? extends Reducer> reducerClass;
	private Path finalOutputDir = null;
	private Class<? extends Mapper> defaultMapperClass = null;
	
	public Job(Configuration config, String jobName) {
		this.config = config;
		this.jobName = jobName;
	}

	public static Job getInstance(Configuration config, String jobName) {
		//This method returns a new instance of Job.
		//The sole reason for creating this method is to mimic the Hadoop API (Job.getInstance).
		return new Job(config, jobName);
	}

	public void setMapperClass(Class<? extends Mapper> mapperClass) {
		//If the task is not yet set(null), replace it with the new mapper
		int size = this.mapTasks.size();
		for (int i=0; i<size; i++) {
			if (this.mapTasks.get(i).getMapperClass() == null) {
				this.mapTasks.get(i).setMapperClass(mapperClass);
			};
		}
		this.defaultMapperClass = mapperClass;
	};
	
	public void addMapTasks(ArrayList<MapTask> mapTasks) {
		this.mapTasks.addAll(mapTasks);
	}

	public ArrayList<MapTask> getMapTasks() throws NoMapperSetException {
		return this.mapTasks;
	}

	protected Class<? extends Mapper> getDefaultMapperClass() {
		return this.defaultMapperClass;
	}

	public void setReducerClass(Class<? extends Reducer> reducerClass) {
		this.reducerClass = reducerClass;
	};
	
	public Class<? extends Reducer> getReducerClass(){
		return this.reducerClass;
	}

	public void setOutputPath(Path finalOutputDir) throws NotDirectoryException {
		if (!Files.isDirectory(finalOutputDir)) {
			throw new NotDirectoryException(finalOutputDir.toAbsolutePath().toString());
		}
		this.finalOutputDir = finalOutputDir;
	};

	public Path getOutputPath() {
		return this.finalOutputDir;
	};
	
	private void check() throws NoMapperSetException, NotDirectoryException {
		// Throw NoMapperSetException if no default mapper is not set and the first 
		int size = this.mapTasks.size();
		for (int i=0; i<size; i++) {
			if (this.mapTasks.get(i).getMapperClass() == null) {
				throw new NoMapperSetException();
			};
		}
	}

	public void waitForCompletion(boolean verbose) throws Exception {
		this.check();
		this.config.check();
		new JobScheduler(this, verbose).start();
	}

}
