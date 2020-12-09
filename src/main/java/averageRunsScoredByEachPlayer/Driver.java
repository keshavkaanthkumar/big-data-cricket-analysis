package averageRunsScoredByEachPlayer;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class Driver {
public static void main(String args[]) {
	Configuration config = new Configuration();
	try {
		Job job = Job.getInstance(config,"combiner");
		job.setJarByClass(Driver.class);
		
		//set mapper and reducer class
		job.setMapperClass(PlayerMapper.class);
		job.setReducerClass(PlayerReducer.class);
		job.setCombinerClass(PlayerReducer.class);
		//set inputformat and output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//set the output key and value class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CountAverageTuple.class);
		
		//set the input and output paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		Path outDir = new Path(args[1]);
		 FileSystem fs = FileSystem.get(job.getConfiguration());
	        if(fs.exists(outDir)){
	            fs.delete(outDir, true);
	        }
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (ClassNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

}
}

