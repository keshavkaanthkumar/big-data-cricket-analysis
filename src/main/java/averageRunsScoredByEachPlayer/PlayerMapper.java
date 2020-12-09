package averageRunsScoredByEachPlayer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class PlayerMapper extends Mapper<LongWritable, Text, Text, CountAverageTuple>{
    private CountAverageTuple outCountAverage = new CountAverageTuple();
    private Text name = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {

        try {

            String input[] = value.toString().split(",");
            String player_name = input[0].trim();

            if (!player_name.isEmpty()&&!input[0].equals("Innings Player")) {
                name.set((player_name));
                outCountAverage.setCount(Long.valueOf(1));
                outCountAverage.setAverage(Float.valueOf(input[2].trim()));
                context.write(name, outCountAverage);
            }

        } catch (Exception e) {
            System.out.println("Something went wrong in Mapper Task: ");
            e.printStackTrace();
        }

    }
}
