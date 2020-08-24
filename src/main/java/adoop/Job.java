package adoop;

import java.nio.file.Path;
import java.util.ArrayList;
import javafx.util.Pair;

public class Job {
	protected Configuration config = null;
	protected String jobName = null;
	protected ArrayList<Pair<Path, Class<? extends Mapper>>> mapTasks = new ArrayList<Pair<Path, Class<? extends Mapper>>>();

	public Job(Configuration config, String jobName) {
		this.config = config;
		this.jobName = jobName;
		//Add a default map task
		this.mapTasks.add(new Pair<Path, Class<? extends Mapper>>(null, null));
	}

	public static Job getInstance(Configuration config, String jobName) {
		return new Job(config, jobName);
	}

	public void setMapper(Class<? extends Mapper> mapperClass) {
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
	
	public void addInputAndMapperPair(Path inputPath, Class<? extends Mapper> mapperClass) {
		//Append a input & mapper pair. This method is intended to be used to configure multiple
		// input files processed by different mappers in a single job.
		this.mapTasks.add(new Pair<Path, Class<? extends Mapper>>(inputPath, mapperClass));
	}

	public ArrayList<Pair<Path, Class<? extends Mapper>>> getMapTasks() {
		return this.mapTasks;
	};

}
