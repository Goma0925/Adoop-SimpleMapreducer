package ao.adoop.mapreduce;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javafx.util.Pair;
import ao.adoop.io.DataLoader;
import ao.adoop.io.FileSystemManager;

public class JobScheduler {
	private Timer timer = new Timer();
	private UserInterface userInterface = null;
	private PriorityQueue<File> reducerInputDirs = new PriorityQueue<File>();
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
		fileManager.clearMapOutputBufferDir();
		
		long threadMaxThreashhold = 30;
		String threadMaxThreashholdUnit = "MB";
		
		//Map
		ArrayList<Pair<Path, Class<? extends Mapper>>> mapTasks = this.job.getMapTasks();
		Class<? extends Mapper> mapperClass = null;
		File inputFile = null;
		for (Pair<Path, Class<? extends Mapper>> mapTask: mapTasks) {
			mapperClass = mapTask.getValue();
			inputFile = mapTask.getKey().toFile();
			this.runMap(mapperClass, inputFile, threadMaxThreashhold, threadMaxThreashholdUnit);
		};
		
		//Reduce
		this.runReduce(this.job.getReducerClass());
		
	};

	public void runMap(Class<? extends Mapper> mapperClass, File inputFile, long threadMaxThreashhold, String threadMaxThreashholdUnit) throws Exception {
		this.timer.startCpuTimer();
		DataLoader loader = new DataLoader();
		ArrayList<int[]> chunkIndices = loader.getChunkIndices(inputFile, threadMaxThreashhold, threadMaxThreashholdUnit);        
		int numberOfThreads = chunkIndices.size();
        
		Mapper[] workers = new Mapper[numberOfThreads];
        Mapper worker = null;
        Constructor<?> mapperConstructor = mapperClass.getDeclaredConstructor(new Class[] {String.class, Configuration.class, File.class, int.class, int.class, String[].class});
        mapperConstructor.setAccessible(true);
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);//creating a pool of X threads  
        
        this.userInterface.doMappingStart(numberOfThreads);
        for (int i = 0; i < chunkIndices.size(); i++) {
        	String workerId = Integer.toString(mapperClass.hashCode()) + "-" + Integer.toString(i);
            String[] namedOutputs = this.job.getNamedOutputs().toArray(new String[job.getNamedOutputs().size()]);
        	//Each mapper worker(thread) will read from the input file, map, and write results to a file.
        	worker = (Mapper) mapperConstructor.newInstance(new Object[] {workerId, this.config, inputFile, chunkIndices.get(i)[0], chunkIndices.get(i)[1], namedOutputs});
        	executor.execute(worker);//Run the thread 
            workers[i] = worker;
	      };
        executor.shutdown(); 
        while (!executor.isTerminated()) {   }  
        userInterface.doMappingEnd();
        timer.stopCpuTimer();
        userInterface.displayRunTime("Map runtime: ", timer.getCpuTimer());   
	};
	
	private void saveReducerInputPaths(File[] mapOutputPaths) {
		
	};
	
	private PriorityQueue<File> loadReducerInputDirs() {
		File[] listOfElements = this.config.mapOutputBufferDir.toFile().listFiles();
		PriorityQueue<File> targetDirs = new PriorityQueue<File>((file1, file2) -> file1.toString().compareTo(file2.toString()));
		for (File ele: listOfElements) {
			if (ele.isDirectory()) {
				targetDirs.add(ele);
			}
		};
		return targetDirs;
	};
	
	public void runReduce(Class<? extends Reducer> reducerClass) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException{
		if (this.reducerInputDirs.size() == 0) {
			//Load the map output directories if they are not in memory.
			this.reducerInputDirs = this.loadReducerInputDirs();
			this.timer.startCpuTimer();
		};

		//Set up the Reduce phase
        int numberOfThreads = this.reducerInputDirs.size();
        this.userInterface.doReducingStart(numberOfThreads);
        Constructor<?> reducerConstructor = reducerClass.getDeclaredConstructor(new Class[] {String.class, Configuration.class, ArrayList.class, String[].class});
        reducerConstructor.setAccessible(true);
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);//creating a pool of 2 threads  
        Reducer[] workers = new  Reducer[numberOfThreads];
        Reducer worker = null;
        String[] namedOutputs = this.job.getNamedOutputs().toArray(new String[job.getNamedOutputs().size()]);
        
        //Run the Reduce phase
        for (int i=0; i<numberOfThreads; i++) {
			ArrayList<File> inputFiles = new ArrayList<File>();
			//Create a list of input files for the reducer from the reducer input directory.
			for (File item: this.reducerInputDirs.remove().listFiles()) {
				if (item.isFile()) {
					inputFiles.add(item);
				}
			};			
        	String id = Integer.toString(i);
        	worker = (Reducer) reducerConstructor.newInstance(new Object[] {id, this.config, inputFiles, namedOutputs});
            executor.execute(worker);//Run the thread 
            workers[i] = worker;
		};
        executor.shutdown(); 
        while (!executor.isTerminated()) {   } ;
        userInterface.doReducingEnd();
        timer.stopCpuTimer();
        userInterface.displayRunTime("Reduce runtime: ", timer.getCpuTimer());  
	}
}
