package ao.adoop.ui;

public class UnknownOptionException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public UnknownOptionException(String option) {
		super("Unknown option:" + option);
	}

}
