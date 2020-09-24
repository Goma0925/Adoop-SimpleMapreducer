package ao.adoop.mapreduce;

import java.nio.file.Path;

public class VerboseInterface implements  UserInterface{
	private Timer totalDuration = new Timer();
	@Override
	public void displayRunTime(String label, double runTime) {
	    System.out.println("	" + label + Double.toString(runTime));
	}

	@Override
	public void doMappingEnd() {
        System.out.println("-------------------Mapping finished-------------------");
	}

	@Override
	public void doMappingStart(int numberOfThreads) {
		System.out.println("--------------------Mapping start---------------------");
		System.out.println("	Mapping running on " + Integer.toString(numberOfThreads) + " threads.");
		System.out.println("	Waiting to finish......");
	}
	
	@Override
	public void doReducingStart(int numberOfThreads) {
		System.out.println("--------------------Reducing start--------------------");
		System.out.println("	Reducing running on " + Integer.toString(numberOfThreads) + " threads.");
	};

	@Override
	public void doReducingEnd() {
        System.out.println("-------------------Reducing finished------------------");
	}

	@Override
	public void doOnExit(Path finalOutputPath) {
        System.out.println("	Output can be found in: " + finalOutputPath.toAbsolutePath().toString());
        System.out.println("------------------------------------------------------");
	}

	@Override
	public void onStart() {
		this.totalDuration.startCpuTimer();
	};
}
