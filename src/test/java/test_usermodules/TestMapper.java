package test_usermodules;

import java.io.File;

import adoop.Context;
import adoop.Mapper;
import settings.SystemPathSettings;

public class TestMapper extends Mapper{


	public TestMapper(String mapperId, SystemPathSettings pathSettings, File inputFile, int startIndex, int endIndex) {
		super(mapperId, pathSettings, inputFile, startIndex, endIndex);
	}

	public void map(String key, String value, Context context) {
		try {
			String[] arrOfStr = value.split("/", 3);
			context.write(arrOfStr[1], arrOfStr[2]);
		} catch (Exception e) {
			System.out.println();
			System.out.println("ERROR:" + e.getMessage()); 
			System.out.println(key+"|"+value);
			System.out.println();
		}
	};
}
