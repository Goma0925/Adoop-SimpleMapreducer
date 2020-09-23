package ao.adoop.mapreduce;

import java.io.File;

public interface UserInterface {
	void displayRunTime(String label, double runTime);
	void displayInputAndMapper(File inputFile, Class<? extends Mapper> mapperClass);
	void displayReducer(Class<? extends Reducer> reducerClass);
	void doMappingStart(int numberOfThreads);
	void doMappingEnd();
	void doReducingStart(int numberOfThreads);
	void doReducingEnd();
	void doOnExit();
}
