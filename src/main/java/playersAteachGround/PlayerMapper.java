package playersAteachGround;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
public class PlayerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{



    private TreeMap<Integer, String> tmap;

    protected void setup(Context context)
            throws IOException,
            InterruptedException{
        tmap = new TreeMap<Integer, String>();
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] tokens = value.toString().split("\t");

        if(!tokens[1].isEmpty()&&!tokens[0].isEmpty())
        {
        	if(isInteger(tokens[1])) {
            int score = Integer.parseInt(tokens[1]);
            String player = tokens[0];

            tmap.put(score,player);

            if(tmap.size()>10){
                tmap.remove(tmap.firstKey());
            }
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

    protected void cleanup(Context context) throws IOException, InterruptedException{
        for(Map.Entry<Integer, String> entry: tmap.entrySet()){

            int score = entry.getKey();
            String player = entry.getValue();

            context.write(new Text(player), new IntWritable(score));
        }
    }
}
