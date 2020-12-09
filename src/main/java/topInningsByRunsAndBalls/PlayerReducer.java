package topInningsByRunsAndBalls;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
public class PlayerReducer extends Reducer<CompositeKey, Text, CompositeKey, Text> {
    protected void reduce(CompositeKey key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer.Context context)
            throws IOException,
            InterruptedException
    {
       for(Text v: values)
       {
           context.write(key,v);
       }
    }
}
