package ao.adoop.mapreduce;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
            Constructor<?> mapperConstructor = mapTask.getMapperClass().getDeclaredConstructor(new Class[] {String.class, Configuration.class, InputSplit.class});
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
	
	private PriorityQueue<File> loadReducerInputDirs() {
		File[] listOfElements = this.config.getMapOutputBufferDir().toFile().listFiles();
		//Use a heap to get the files in order by file name.
		PriorityQueue<File> targetDirs = new PriorityQueue<File>((file1, file2) -> file1.toString().compareTo(file2.toString()));
		for (File ele: listOfElements) {
			if (ele.isDirectory()) {
				targetDirs.add(ele);
			}
		};
		return targetDirs;
	};
	
	public void runReduce(Class<? extends Reducer> reducerClass) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException{
		this.timer.startCpuTimer();

		//Set up the Reduce phase
		PriorityQueue<File> reducerInputDirs = this.loadReducerInputDirs();
        int taskNum = reducerInputDirs.size();
        //Start reducing if there are any map outputs.
        if (taskNum >= 0) {
            this.userInterface.doReducingStart(taskNum);
            Constructor<?> reducerConstructor = reducerClass.getDeclaredConstructor(new Class[] {String.class, Configuration.class, ArrayList.class});
            reducerConstructor.setAccessible(true);
            ExecutorService executor = Executors.newFixedThreadPool(this.config.getMaxThreadNum());//creating a pool of 2 threads  
            Reducer[] workers = new  Reducer[taskNum];
            Reducer worker = null;
            
            //Run the Reduce phase
            for (int i=0; i<taskNum; i++) {
    			ArrayList<File> inputFiles = new ArrayList<File>();
    			//Create a list of input files for the reducer from the reducer input directory.
    			for (File item: reducerInputDirs.remove().listFiles()) {
    				if (item.isFile()) {
    					inputFiles.add(item);
    				}
    			};
            	String id = Integer.toString(reducerClass.hashCode()) + "-" + Integer.toString(i);
            	worker = (Reducer) reducerConstructor.newInstance(new Object[] {id, this.config, inputFiles});
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
