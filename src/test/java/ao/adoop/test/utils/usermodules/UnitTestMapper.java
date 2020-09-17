package ao.adoop.test.utils.usermodules;

import java.io.File;

import ao.adoop.mapreduce.Configuration;
import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.Mapper;

public class UnitTestMapper extends Mapper{


	public UnitTestMapper(String workerId, Configuration config, File inputFile, Integer startIndex,
			Integer endIndex, String[] addedNamedOutputs) {
		super(workerId, config, inputFile, startIndex, endIndex, addedNamedOutputs);
	}

	public void map(String key, String value, Context context) {
		String[] arrOfStr = value.split("/", 3);
		context.write(arrOfStr[1], arrOfStr[2]);
	};
}