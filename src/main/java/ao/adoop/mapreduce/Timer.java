package ao.adoop.mapreduce;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class Timer {
	private ThreadMXBean TMB = ManagementFactory.getThreadMXBean();
	private long startTime;
	private long endTime;
		
	public void startCpuTimer() {
		this.startTime = TMB.getCurrentThreadCpuTime();
	}
	
	public void stopCpuTimer() {
		this.endTime = TMB.getCurrentThreadCpuTime();
	};
	
	public double getCpuTimer() {
	    return (double) (endTime - startTime) / 1000000000;
	};
}
