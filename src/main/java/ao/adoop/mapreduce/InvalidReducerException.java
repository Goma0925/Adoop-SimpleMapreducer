package ao.adoop.mapreduce;

public class InvalidReducerException extends Exception{
	private static final long serialVersionUID = 4862618313133993820L;
	public InvalidReducerException(Class<?> reducerClass) {
        super("Reducer Class '" + reducerClass.getName() + "' should extend Reducer class.");
    }
}

