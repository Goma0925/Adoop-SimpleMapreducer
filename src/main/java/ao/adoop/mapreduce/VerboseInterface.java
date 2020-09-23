package ao.adoop.mapreduce;

import java.io.File;
import java.nio.file.Path;

public class VerboseInterface implements  UserInterface{
	@Override
	public void displayRunTime(String label, double runTime) {
	    System.out.println("	" + label + Double.toString(runTime));
	}

	@Override
	public void doMappingEnd() {
        System.out.println("-------Mapping finished-------");
	}

	@Override
	public void doMappingStart(int numberOfThreads) {
		System.out.println("--------Mapping start---------");
		System.out.println("	Mapping running on " + Integer.toString(numberOfThreads) + " threads.");
	}
	
	@Override
	public void doReducingStart(int numberOfThreads) {
		System.out.println("--------Reducing start--------");
		System.out.println("	Reducing running on " + Integer.toString(numberOfThreads) + " threads.");
	};

	@Override
	public void doReducingEnd() {
        System.out.println("-------Reducing finished------");
	}

	@Override
	public void doOnExit() {
        System.out.println("------------------------------");
	}

	@Override
	public void displayInputAndMapper(File inputFile, Class<? extends Mapper> mapperClass) {
        System.out.println("	┌ Mapper : " + mapperClass.toString());
        System.out.println("	└ Input  : " + inputFile.getAbsolutePath());
	}

	@Override
	public void displayReducer(Class<? extends Reducer> reducerClass) {
        System.out.println("	[ Reducer: " + reducerClass.toString());
	}
}
