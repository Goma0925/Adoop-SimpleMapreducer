package ao.adoop.mapreduce;

import javafx.util.Pair;

public class CommandLineInterface implements  UserInterface{
	public void displayRunTime(String label, double runTime) {
	    System.out.println(label + Double.toString(runTime));
	}

	public void doMappingEnd() {
        System.out.println("Mapping finished.");
	}

	public void doMappingStart(int numberOfThreads) {
		System.out.println("---Concurrent Mapping start---");
		System.out.println("	Mapping running on " + Integer.toString(numberOfThreads) + " threads.");
	}

	public void doReducingStart(int numberOfThreads) {
		System.out.println("---Concurrent Reducing start---");
		System.out.println("	Reducing running on " + Integer.toString(numberOfThreads) + " threads.");
	};

	public void doReducingEnd() {
        System.out.println("Reducing finished.");
	}

	public Pair<String, String[]> acceptCommand() {
		return null;
	};
}
