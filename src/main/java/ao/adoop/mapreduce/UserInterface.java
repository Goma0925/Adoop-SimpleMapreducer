package ao.adoop.mapreduce;

public interface UserInterface {
	void displayRunTime(String label, double runTime);
	void doMappingStart(int numberOfThreads);
	void doMappingEnd();
	void doReducingStart(int numberOfThreads);
	void doReducingEnd();
}
