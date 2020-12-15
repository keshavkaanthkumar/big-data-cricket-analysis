package playersAteachGround;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;

public class PlayerCountMapper extends Mapper<LongWritable,Text,  Text, IntWritable> {
	 private Text word = new Text();
	    private IntWritable one = new IntWritable(1);

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	        try {
	        	
	            String input[] = value.toString().split(",");
	            if(!input[0].equals("Innings Player")){
	            String player_name = input[0].trim();
                String ground= input[12].trim();
	            word.set(player_name+","+ground);
	            context.write(word, one);
	            }
	        } catch (Exception e) {
	            System.out.println("Something went wrong in Mapper Task: ");
	            e.printStackTrace();
	        }

	    }
    }
