package ao.adoop.mapreduce;

import java.nio.file.Path;

public interface UserInterface {
	void displayRunTime(String label, double runTime);
	void onStart();
	void doMappingStart(int numberOfThreads);
	void doMappingEnd();
	void doReducingStart(int numberOfThreads);
	void doReducingEnd();
	void doOnExit(Path finalOutputDir);
}
