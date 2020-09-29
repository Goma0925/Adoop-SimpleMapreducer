package ao.adoop.mapreduce;

public class Timer {
	private long startTime;
	private long endTime;
		
	public void startCpuTimer() {
		this.startTime = System.currentTimeMillis();
	}
	
	public void stopCpuTimer() {
		this.endTime = System.currentTimeMillis();
	};
	
	public double getCpuTimerInSecs() {
	    return (double) (endTime - startTime) / 1000;
	};
}
