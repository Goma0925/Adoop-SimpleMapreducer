package ao.adoop.mapreduce;

import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;
import javafx.util.Pair;

public class Job {
	protected Configuration config = null;
	protected String jobName = null;
	protected ArrayList<Pair<Path, Class<? extends Mapper>>> mapTasks = new ArrayList<Pair<Path, Class<? extends Mapper>>>();
	private Class<? extends Reducer> reducerClass;
	private ArrayList<String> outputNamedOutputs = null;//Outputs in each name space is written to different files.
	private Path finalOutputDir = null;
	
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
		//Set a mapper class for the first map task.
		if (mapTasks.size() == 0) {
			//Create a new mapTask if it doesn't exist.
			this.mapTasks.add(new Pair<Path, Class<? extends Mapper>>(null, null));
		}
		Pair<Path, Class<? extends Mapper>> mapTask = this.mapTasks.get(0);
		Pair<Path, Class<? extends Mapper>> updatedMapTask = null;
		if (mapTask.getValue() == null) {
			//Create a new pair of Input file and Mapper mapping if the mapper isn't set. 
			updatedMapTask = new Pair<Path, Class<? extends Mapper>>(mapTask.getKey(), mapperClass);
		};
		this.mapTasks.set(0, updatedMapTask);
	};
	
	public void setInputPath(Path inputPath) {
		//Set a input path for the first map task.
		if (mapTasks.size() == 0) {
			//Create a new mapTask if it doesn't exist.
			this.mapTasks.add(new Pair<Path, Class<? extends Mapper>>(null, null));
		}
		Pair<Path, Class<? extends Mapper>> mapTask = this.mapTasks.get(0);
		Pair<Path, Class<? extends Mapper>> updatedMapTask = null;
		if (mapTask.getKey() == null) {
			//Create a new pair of Input file and Mapper mapping if the inputPath isn't set. 
			updatedMapTask = new Pair<Path, Class<? extends Mapper>>(inputPath, mapTask.getValue());
		};
		this.mapTasks.set(0, updatedMapTask);
	};
	
	protected void addInputAndMapperPair(Path inputPath, Class<? extends Mapper> mapperClass) {
		//Append a input & mapper pair. This method is intended to be used to configure multiple
		// input files, each processed by different mappers in a single job.
		// This method is only accessible from MutipleInputs class.
		this.mapTasks.add(new Pair<Path, Class<? extends Mapper>>(inputPath, mapperClass));
	}

	public ArrayList<Pair<Path, Class<? extends Mapper>>> getMapTasks() {
		return this.mapTasks;
	}

	public void setReducerClass(Class<? extends Reducer> reducerClass) {
		this.reducerClass = reducerClass;
	};
	
	public Class<? extends Reducer> getReducerClass(){
		return this.reducerClass;
	}

	protected void addNamedOutput(String outputNamedOutput) {
		if (this.outputNamedOutputs == null) {
			this.outputNamedOutputs = new ArrayList<String>();
		};
		this.outputNamedOutputs.add(outputNamedOutput);
	}
	
	public ArrayList<String> getNamedOutputs(){
		return this.outputNamedOutputs;
	}

	public void setOutputPath(Path finalOutputDir) {
		if (!finalOutputDir.toFile().isDirectory()) {
			this.finalOutputDir = finalOutputDir;
		}else {
			new NotDirectoryException("The output path must be a directory: " + finalOutputDir.toString());
		}
	}

	public Path getOutputPath() {
		return this.finalOutputDir;
	};
	

	public void waitForCompletion(boolean verbose) throws Exception {
		new JobScheduler(this, verbose).start();
	}

}
