package ao.adoop.mapreduce;

import javafx.util.Pair;

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

	public Pair<String, String[]> acceptCommand() {
		return null;
	};
}
