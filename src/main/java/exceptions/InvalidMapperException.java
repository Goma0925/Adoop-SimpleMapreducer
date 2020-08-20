package exceptions;

public class InvalidMapperException extends Exception {
	private static final long serialVersionUID = 1L;
	public InvalidMapperException(Class<?> mapperClass) {
        super("Mapper Class '" + mapperClass.getName() + "' should extend Mapper class.");
    }
}
