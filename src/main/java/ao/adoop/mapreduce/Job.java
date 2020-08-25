package ao.adoop.mapreduce;

import java.nio.file.Path;
import java.util.ArrayList;
import javafx.util.Pair;

public class Job {
	protected Configuration config = null;
	protected String jobName = null;
	protected ArrayList<Pair<Path, Class<? extends Mapper>>> mapTasks = new ArrayList<Pair<Path, Class<? extends Mapper>>>();
	private Class<? extends Reducer> reducerClass;

	public Job(Configuration config, String jobName) {
		this.config = config;
		this.jobName = jobName;
		//Add a default map task
		this.mapTasks.add(new Pair<Path, Class<? extends Mapper>>(null, null));
	}

	public static Job getInstance(Configuration config, String jobName) {
		//This method returns a new instance of Job.
		//The sole reason for creating this method is to mimic the Hadoop API (Job.getInstance).
		return new Job(config, jobName);
	}

	public void setMapperClass(Class<? extends Mapper> mapperClass) {
		//Set a mapper class for the first map task.
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

}
