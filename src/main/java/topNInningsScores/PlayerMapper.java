package topNInningsScores;

import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PlayerMapper  extends Mapper<LongWritable, Text, IntWritable, Text> {
HashMap<String ,Integer> hashmap=new HashMap<String,Integer>();
    public void map(LongWritable key, Text value, Context context){

        String[] row = value.toString().split(",");
        String playerName = row[0].trim();
        if(!row[6].trim().isEmpty()&&isInteger(row[6])) {
            int inningsScore = Integer.parseInt(row[6].trim());
        	if(!(hashmap.containsKey(playerName)&&hashmap.get(playerName)==inningsScore))
        	{
        		
        	hashmap.put(playerName, inningsScore);
        try{
            Text name = new Text(playerName );
            IntWritable score = new IntWritable(inningsScore);
            
            context.write(score, name);

        }catch(Exception e){
            System.out.println("Something went wrong in Mapper Task: ");
            e.printStackTrace();
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
}
