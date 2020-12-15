package playersAteachGround;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class PlayerReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	 private TreeMap<Integer , String> tmap2;

	    protected void setup(Context context)
	            throws IOException,
	            InterruptedException
	    {
	        tmap2 = new TreeMap<Integer, String>();
	    }

	    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
	            throws IOException,
	            InterruptedException
	    {
	        String playerground = key.toString();
	        int score = 0;

	        for(IntWritable val : values)
	        {
	            score= val.get();
	            tmap2.put(score,playerground);
	            if(tmap2.size()>10){
	                tmap2.remove(tmap2.firstKey());
	            }
	        }
	    }

	    protected void cleanup(Context context)
	            throws IOException,
	            InterruptedException
	    {
	        for(Map.Entry<Integer, String> entry : tmap2.entrySet())
	        {
	            String playerground = entry.getValue();
	             int score = entry.getKey();
	            context.write(new Text(playerground), new IntWritable(score));
	        }
	    }
}
