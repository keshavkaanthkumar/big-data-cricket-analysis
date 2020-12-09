package minMaxMeanByGround;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class CustomOutputTuple implements Writable {
    private int min;
    private int max;
    private long count;
    private int median;
    private int stdDev;
    public CustomOutputTuple() {
    }
    public CustomOutputTuple(int min, int max, long count, int median, int stdDev) {
        this.min = min;
        this.max = max;
        this.count = count;
        this.median = median;
        this.stdDev = stdDev;
    }

    public int getMin() {
		return min;
	}
	public void setMin(int min) {
		this.min = min;
	}
	public int getMax() {
		return max;
	}
	public void setMax(int max) {
		this.max = max;
	}
	public long getCount() {
        return count;
    }
    public void setCount(long count) {
        this.count = count;
    }
 
    public int getMedian() {
		return median;
	}
	public void setMedian(int median) {
		this.median = median;
	}
	public int getStdDev() {
		return stdDev;
	}
	public void setStdDev(int stdDev) {
		this.stdDev = stdDev;
	}
	public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(min);
        dataOutput.writeInt(max);
        dataOutput.writeLong(count);
        dataOutput.writeInt(median);
        dataOutput.writeInt(stdDev);
    }
    public void readFields(DataInput dataInput) throws IOException {
        min = dataInput.readInt();
        max = dataInput.readInt();
        count = dataInput.readLong();
        median = dataInput.readInt();
        stdDev = dataInput.readInt();
    }
    public String toString(){
        return "\t" +"min: "+min + "\t" +"max: "+ max + "\t"+ "count: "  + count + "\t" +"median: "+ median + "\t" +"stdDev: "+ stdDev; 
    }
}
