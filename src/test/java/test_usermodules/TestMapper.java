package test_usermodules;

import adoop.Context;
import adoop.Mapper;

public class TestMapper implements Mapper{


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
