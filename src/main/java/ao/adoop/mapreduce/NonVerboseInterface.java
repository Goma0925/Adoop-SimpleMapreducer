package ao.adoop.mapreduce;

public class NonVerboseInterface implements  UserInterface{
	public void displayRunTime(String label, double runTime) {
	}

	public void doMappingEnd() {
	}

	public void doMappingStart(int numberOfThreads) {
	}

	public void doReducingStart(int numberOfThreads) {
	};

	public void doReducingEnd() {
	}

	@Override
	public void doOnExit() {		
	};
}
