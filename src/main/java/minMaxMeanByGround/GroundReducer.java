package minMaxMeanByGround;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GroundReducer extends Reducer<Text, MinMaxTuple, Text, CustomOutputTuple> {
    CustomOutputTuple result = new CustomOutputTuple();
    List<Integer> scores = new ArrayList<Integer>();
    protected void reduce(Text key, Iterable<MinMaxTuple> values, Context context)
            throws IOException,
            InterruptedException, IOException {
        result.setMin(Integer.MAX_VALUE);
        result.setMax(Integer.MIN_VALUE);
        result.setCount(0);
        result.setMedian(0);
        result.setStdDev(0);
        long count = 0;
        double sum = 0.0;
        for(MinMaxTuple minMaxTuple : values)
        {
            sum += minMaxTuple.getMax();
            scores.add(minMaxTuple.getMax());
            if(result.getMin() == Integer.MAX_VALUE || result.getMin()>minMaxTuple.getMin()){
                result.setMin(minMaxTuple.getMin());
            }
            if(result.getMax() == Integer.MIN_VALUE || minMaxTuple.getMax()>result.getMax()){
                result.setMax(minMaxTuple.getMax());
            }
            count+= minMaxTuple.getCount();
        }
        result.setCount(count);
        Collections.sort(scores);
        int len = scores.size();
        if(len%2!=0){
            result.setMedian(scores.get(len/2));
        }
        else{
            result.setMedian((scores.get((len-1)/2)+scores.get(len/2))/2);
        }
        double mean = sum/count;
        double stdDev = 0.0;
        for(int salary : scores)
        {
            stdDev += (salary-mean)*(salary-mean);
        }
        result.setStdDev((int)Math.sqrt(stdDev /len));
        context.write(key, result);
    }
}






