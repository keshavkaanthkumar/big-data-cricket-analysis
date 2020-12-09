package noOfCenturiesByGround;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class GroundMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
    public static final String GROUND = "ground";
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] tokens = value.toString().split(",");
        if(!tokens[12].isEmpty())
        {
        	if(isInteger(tokens[2])&&Integer.parseInt(tokens[2])>99){
            String ground = tokens[12];
            context.getCounter(GROUND, ground).increment(1);
        	}
        }
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
