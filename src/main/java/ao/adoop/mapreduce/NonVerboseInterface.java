package ao.adoop.mapreduce;

import java.nio.file.Path;

public class NonVerboseInterface implements  UserInterface{

	@Override
	public void displayRunTime(String label, double runTime) {
		
	}

	@Override
	public void doMappingStart(int numberOfThreads) {
		
	}

	@Override
	public void doMappingEnd() {
		
	}

	@Override
	public void doReducingStart(int numberOfThreads) {
		
	}

	@Override
	public void doReducingEnd() {
		
	}

	@Override
	public void doOnExit(Path finalOutputPath) {
		
	}

	@Override
	public void onStart() {
		
	}

}
