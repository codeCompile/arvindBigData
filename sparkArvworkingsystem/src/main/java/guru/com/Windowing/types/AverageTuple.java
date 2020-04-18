package guru.com.Windowing.types;

import java.io.Serializable;

public class AverageTuple implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 332323;

	private int count;

	private double average;

	public AverageTuple(int count, double average) {
		super();
		this.count = count;
		this.average = average;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public double getAverage() {
		return average;
	}

	public void setAverage(double average) {
		this.average = average;
	}

	@Override
	public String toString() {
		return String.valueOf(average/count);
	}

}