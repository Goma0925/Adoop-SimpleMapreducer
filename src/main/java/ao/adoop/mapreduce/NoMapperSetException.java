package ao.adoop.mapreduce;

public class NoMapperSetException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NoMapperSetException() {
        super("Mapper is not set. Please set a mapper by using Job.setMapper().");
    }
}
