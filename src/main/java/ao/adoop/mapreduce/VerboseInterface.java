package ao.adoop.mapreduce;

public class VerboseInterface implements  UserInterface{
	public void displayRunTime(String label, double runTime) {
	    System.out.println("	" + label + Double.toString(runTime));
	}

	public void doMappingEnd() {
        System.out.println("-------Mapping finished-------");
	}

	public void doMappingStart(int numberOfThreads) {
		System.out.println("--------Mapping start---------");
		System.out.println("	Mapping running on " + Integer.toString(numberOfThreads) + " threads.");
	}

	public void doReducingStart(int numberOfThreads) {
		System.out.println("--------Reducing start--------");
		System.out.println("	Reducing running on " + Integer.toString(numberOfThreads) + " threads.");
	};

	public void doReducingEnd() {
        System.out.println("-------Reducing finished------");
	}

	@Override
	public void doOnExit() {
        System.out.println("------------------------------");
	}
}
