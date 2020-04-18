package guru.com.Windowing.types;

import java.io.Serializable;

public class RelativeTuple implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 54545454;

	private int upwardCount;

	private int downwardCount;

	private double upward;

	private double downward;

	public RelativeTuple(int upwardCount, int downwardCount, double upward,
			double downward) {
		super();
		this.upwardCount = upwardCount;
		this.downwardCount = downwardCount;
		this.upward = upward;
		this.downward = downward;
	}

	public int getUpwardCount() {
		return upwardCount;
	}

	public void setUpwardCount(int upwardCount) {
		this.upwardCount = upwardCount;
	}

	public int getDownwardCount() {
		return downwardCount;
	}

	public void setDownwardCount(int downwardCount) {
		this.downwardCount = downwardCount;
	}

	public double getUpward() {
		return upward;
	}

	public void setUpward(double upward) {
		this.upward = upward;
	}

	public double getDownward() {
		return downward;
	}

	public void setDownward(double downward) {
		this.downward = downward;
	}

	@Override
	public String toString() {

		Double averageGain = 0d;
		Double averageLoss = 0d;
		Double rs = 0d;

		if (upward > 0) {
			averageGain = upward / upwardCount;
		}

		if (downward > 0) {
			averageLoss = downward / downwardCount;
		}

		if (averageGain > 0 && averageLoss > 0) {
			rs = averageGain / averageLoss;
		} else if (averageGain == 0) {
			rs = 0d;
		} else if (averageLoss == 0) {
			rs = 100d;
		}

		return String.valueOf(100 - 100 / (1 + rs));
	}

}