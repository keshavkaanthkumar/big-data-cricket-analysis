package minMaxMeanByGround;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class GroundMapper extends Mapper<LongWritable, Text, Text, MinMaxTuple > {
    MinMaxTuple minMaxTuple = new MinMaxTuple();
    Text ground = new Text();
    protected void map(LongWritable key, Text value, Context context)
            throws IOException,
            InterruptedException, IOException {
        String line = value.toString();
        String[] tokens = line.split(",");
        int score=0;
        if(tokens[12].length()>0&&isInteger(tokens[2])){
            score = Integer.parseInt(tokens[2]);
            minMaxTuple.setMin(score);
            minMaxTuple.setMax(score);
            minMaxTuple.setCount(1);
            ground.set(tokens[12]);
        }
        context.write(ground, minMaxTuple);
    }
    public static boolean isInteger(String s) {
        try { 
            Integer.parseInt(s); 
        } catch(NumberFormatException e) { 
            return false; 
        } catch(NullPointerException e) {
            return false;
        }
        return true;
    }
}
