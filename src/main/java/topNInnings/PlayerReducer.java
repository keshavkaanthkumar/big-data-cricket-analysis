package topNInnings;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class PlayerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private TreeMap<Integer, String> tmap2;

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
        String player = key.toString();
        int score = 0;

        for(IntWritable val : values)
        {
            score= val.get();
            tmap2.put(score, player );
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
            int score = entry.getKey();
            String player = entry.getValue();
            context.write(new Text(player), new IntWritable(score));
        }
    }

}
