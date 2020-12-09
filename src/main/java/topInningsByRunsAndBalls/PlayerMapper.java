package topInningsByRunsAndBalls;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PlayerMapper extends Mapper<LongWritable, Text, CompositeKey, NullWritable> {
    protected void map(LongWritable key,
            Text value,
            org.apache.hadoop.mapreduce.Mapper.Context context)
 throws IOException,
 InterruptedException{
String line  = value.toString();
String[] tokens = line.split(",");
String name = null;
String score = null;
String ballsFaced=null;
name=tokens[0];
score=tokens[2];
ballsFaced=tokens[6];
Text playerName = new Text();
if(name!=null && score!=null&&ballsFaced!=null)
{
	if(isInteger(score)&&(isInteger(ballsFaced))) {
 CompositeKey outKey = new CompositeKey(score, ballsFaced);
 playerName.set(name);
 context.write(outKey, playerName );
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
