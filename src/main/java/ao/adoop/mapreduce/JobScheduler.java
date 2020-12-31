package ao.adoop.mapreduce;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ao.adoop.io.FileSystemManager;

public class JobScheduler {
	private Timer timer = new Timer();
	private UserInterface userInterface = null;
	private Job job = null;
	private Configuration config = null;
	
	public JobScheduler(Job job, boolean verboseOn) throws Exception {
		this.job = job;
		if (verboseOn) {
			this.userInterface = new VerboseInterface();
		}else {
			this.userInterface = new NonVerboseInterface();
		};
		this.config = job.config;
	}

	protected void start() throws Exception{
		FileSystemManager fileManager = new FileSystemManager(this.config);
		fileManager.initFileSystem();
		fileManager.clearMapOutputBufferDir();
		
		//Map
		this.runMap(this.job.getMapTasks());

		//Reduce
		this.runReduce(this.job.getReducerClass());
		this.userInterface.doOnExit(this.job.config.getFinalOutputDir());
		fileManager.clearMapOutputBufferDir();
	};

	public void runMap(ArrayList<MapTask> mapTasks) throws Exception {
		this.timer.startCpuTimer();
        
		//Set up mapper threads
        int taskNum = mapTasks.size();
		this.userInterface.doMappingStart(this.config.getMaxThreadNum());
		Mapper[] workers = new Mapper[taskNum];
        Mapper worker = null;
        MapTask mapTask = null;
        ExecutorService executor = Executors.newFixedThreadPool(this.config.getMaxThreadNum());//creating a pool of X threads  
        
        for (int i = 0; i < taskNum; i++) {
        	//Each mapper worker(thread) will read from the input file, map, and write results to a file.
        	mapTask = mapTasks.get(i);
        	String workerId = mapTask.hashCode() + "-" + Integer.toString(i);
            Constructor<?> mapperConstructor = mapTask.getMapperClass().getDeclaredConstructor(new Class[] {String.class, Configuration.class, MapInputSplit.class});
            mapperConstructor.setAccessible(true);
        	worker = (Mapper) mapperConstructor.newInstance(new Object[] {workerId, this.config, mapTask.getInputSplit()});
        	executor.execute(worker);//Run the thread 
            workers[i] = worker;
	      };
        executor.shutdown(); 
        while (!executor.isTerminated()) { Thread.sleep(200);   };
        this.timer.stopCpuTimer();
        this.userInterface.doMappingEnd();
        this.userInterface.displayRunTime("Map runtime: ", timer.getCpuTimerInSecs());   
	};
	
	private PriorityQueue<Path> loadReducerInputDirs() throws IOException {
		//Use a heap to get the files in order by file name.
		Path mapOutputBufferDir = this.config.getMapOutputBufferDir().toAbsolutePath();
		PriorityQueue<Path> targetDirs = new PriorityQueue<Path>((path1, path2) -> path1.toString().compareTo(path2.toString()));
		try (Stream<Path> walk = Files.walk(mapOutputBufferDir)) {
			List<Path> result = walk.filter(Files::isDirectory).collect(Collectors.toList());
			result.forEach((Path path)->{
				//Do not include the mapOutputBufferDir in targetDirs.
				if (!path.toAbsolutePath().toString().equals(mapOutputBufferDir.toString())) {
					targetDirs.add(path);
				}});
		} catch (IOException e) {
			e.printStackTrace();
		};
		return targetDirs;
	};
	
	private Path[] getFilesIn(Path dir){
		ArrayList<Path> list = new ArrayList<>();
		try (Stream<Path> walk = Files.walk(dir)) {
			List<Path> result = walk.filter(Files::isRegularFile).collect(Collectors.toList());
			result.forEach((Path path)->{list.add(path);});
		} catch (IOException e) {
			e.printStackTrace();
		};
		return list.toArray(new Path[list.size()]);
	}
	
	public void runReduce(Class<? extends Reducer> reducerClass) throws IOException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException{
		this.timer.startCpuTimer();

		//Set up the Reduce phase
		PriorityQueue<Path> reducerInputDirs = this.loadReducerInputDirs();
        int taskNum = reducerInputDirs.size();
        //Start reducing if there are any map outputs.
        if (taskNum >= 0) {
            this.userInterface.doReducingStart(taskNum);
            Constructor<?> reducerConstructor = reducerClass.getDeclaredConstructor(new Class[] {String.class, Configuration.class, ReduceInputSplit.class});
            reducerConstructor.setAccessible(true);
            ExecutorService executor = Executors.newFixedThreadPool(this.config.getMaxThreadNum());//creating a pool of 2 threads  
            Reducer[] workers = new  Reducer[taskNum];
            Reducer worker = null;
            
            //Run the Reduce phase
            for (int i=0; i<taskNum; i++) {
    			//Create a list of input files for the reducer from the reducer input directory.
            	String id = Integer.toString(reducerClass.hashCode()) + "-" + Integer.toString(i);
            	Path keyDir = reducerInputDirs.remove();
            	worker = (Reducer) reducerConstructor.newInstance(new Object[] {id, this.config, new ReduceInputSplit(this.getFilesIn(keyDir))});
                executor.execute(worker);//Run the thread 
                workers[i] = worker;
    		};
            executor.shutdown(); 
            while (!executor.isTerminated()) { Thread.sleep(200); } ;
            timer.stopCpuTimer();
            this.userInterface.doReducingEnd();
            this.userInterface.displayRunTime("Reduce runtime: ", timer.getCpuTimerInSecs()); 
        }
	}
}
