package adooptest;

import java.util.ArrayList;
import java.util.Map;

import javax.naming.InvalidNameException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import ao.adoop.mapreduce.Context;
import ao.adoop.mapreduce.Job;
import ao.adoop.mapreduce.MultipleOutputs;

class ContextTest {
	String[] testValueSet1 = {"value1", "value2", "value3", "value4", "value5",
						"value6", "value7", "value8", "value9", "value10"};
	String[] testValueSet2 = {"hello1", "hello2", "hello3", "hello4", "hello5", 
						"hello6", "hello7", "hello8", "hello9", "hello10", };
	String[] testValueSet3 = {"hi1", "hi2", "hi3", "hi4", "hi5", 
						"hi6", "hi7", "hi8", "hi9", "hi10", };
	String[][] valueSets = {testValueSet1, testValueSet2, testValueSet3};
	
	@Test
	void testWrite() {
		Context context = new Context();
		
		//Write to the default output name space
		String key = "";
		for (int i=0; i<testValueSet1.length; i++) {
			//Put items at an odd number index with key "1", items at an even number index with key "0"
			key = Integer.toString(i%2);
			context.write(key, testValueSet1[i]);
		}
		
		//Get the map of a default name space that stores the written results.
		Map<String, ArrayList<String>> keyValMapping = context.getDefaultMapping();
		
		//Check the results for key "0"
		key = "0";
		ArrayList<String> valueList = keyValMapping.get(key);
		ArrayList<String> answerValueList = getItemsAtEvenIndex(testValueSet1);
		Assertions.assertArrayEquals(answerValueList.toArray(), valueList.toArray());
		
		//Check the results for key "1"
		key = "1";
		valueList = keyValMapping.get(key);
		answerValueList = getItemsAtOddIndex(testValueSet1);
		Assertions.assertArrayEquals(answerValueList.toArray(), valueList.toArray());
	};
	
	@Test
	void testWriteToNamedOutput() throws InvalidNameException{
		//To write output to different locations, use MutipleInputs wrapper with Context.
		//This API is a mimic from the Hadoop API.
		ArrayList<String> namedOutputs = new ArrayList<String>();
		Context context = new Context();
		
		//Set namedOutputs to store key & val pairs.
		for (int setNum=0; setNum<valueSets.length; setNum++) {
			String namedOutput = "Set" + Integer.toString(setNum);
			namedOutputs.add(namedOutput);
		};
		
		context.setNamedOutputs(namedOutputs);
		MultipleOutputs multipleOutputs = new MultipleOutputs(context);
		
		//Check if writing to an unregistered namedOutput fails.
		Assertions.assertThrows(InvalidNameException.class, () -> {
			multipleOutputs.write("Unregistered name", "key", "value", "test/path");				
		});
		
		String key = "";
		for (int setNum=0; setNum<valueSets.length; setNum++) {
			String namedOutput = namedOutputs.get(setNum);
			String baseOutputPath = "/"+namedOutput;
			for (int i=0; i<valueSets[setNum].length; i++) {
				//Put items at an odd number index with key "1", items at an even number index with key "0"
				key = Integer.toString(i%2);
				multipleOutputs.write(namedOutput, key, valueSets[setNum][i], baseOutputPath);				
			}
		};
		
		//Check if the context contains all the values in the corresponding set for each name space.
		for (int setNum=0; setNum<valueSets.length; setNum++) {
			//Get the keyValMapping for each name space.
			String namedOutput = "Set" + Integer.toString(setNum);
			Map<String, ArrayList<String>> keyValMapping = context.getNamedMapping(namedOutput);
			
			//Check the results for key "0"
			key = "0";
			ArrayList<String> valueList = keyValMapping.get(key);
			ArrayList<String> answerValueList = getItemsAtEvenIndex(valueSets[setNum]);
			Assertions.assertArrayEquals(answerValueList.toArray(), valueList.toArray());
			
			//Check the results for key "1"
			key = "1";
			valueList = keyValMapping.get(key);
			answerValueList = getItemsAtOddIndex(valueSets[setNum]);
			Assertions.assertArrayEquals(answerValueList.toArray(), valueList.toArray());
		};
		
		//Check if the context contains the correct baseOutputPath for each name space.
		for (String namedOutput: namedOutputs) {
			String baseOutputPath = context.getBaseOutputPath(namedOutput);
			Assertions.assertEquals("/"+namedOutput, baseOutputPath);
		}
	}
	
	ArrayList<String> getItemsAtOddIndex(String[] array) {
		ArrayList<String> results = new ArrayList<String>();
		for(int j=0; j != array.length; j++) {
		    if (j % 2 == 1) { // Odd
		    	results.add(array[j]); 
		    }
		}
		return results;
	}
	
	ArrayList<String> getItemsAtEvenIndex(String[] array) {
		ArrayList<String> results = new ArrayList<String>();
		for(int j=0; j != array.length; j++) {
		    if (j % 2 == 0) { // Even
		    	results.add(array[j]); 
		    }
		}
		return results;
	}

}
