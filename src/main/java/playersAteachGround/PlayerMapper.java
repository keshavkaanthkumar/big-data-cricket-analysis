package playersAteachGround;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class PlayerMapper extends Mapper<LongWritable, Text, Text, Text>{
    private Text ground = new Text();
    private Text player = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if(key.get()==0){
            return;
        }
        try{
            String[] tokens = value.toString().split(",");
            ground.set(tokens[12]);
            player.set(tokens[0]);
            context.write(ground, player);

        } catch(Exception  e){
            System.out.println("Something went wrong in Mapper Task: ");
            e.printStackTrace();
        }
    }
}
