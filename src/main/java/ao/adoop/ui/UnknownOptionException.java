package ao.adoop.ui;

public class UnknownOptionException extends Exception {

	public UnknownOptionException(String option) {
		super("Unknown option:" + option);
	}

}
