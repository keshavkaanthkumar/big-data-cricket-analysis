package minMaxMeanByGround;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxTuple implements Writable {
    private int min;
    private int max;
    private long count;
    public MinMaxTuple() {
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(min);
        out.writeInt(max);
        out.writeLong(count);
    }
    public void readFields(DataInput in) throws IOException {
        min = in.readInt();
        max = in.readInt();
        count = in.readLong();
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
    public String toString()
    {
        return max + "\t" + min + "\t" + count;
    }
}


