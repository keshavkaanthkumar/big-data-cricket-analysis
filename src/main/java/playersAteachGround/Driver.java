package playersAteachGround;

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
			Job job = Job.getInstance(config,"Map reduce Chaining");
			job.setJarByClass(Driver.class);
			
			//set mapper and reducer class
			job.setMapperClass(PlayerCountMapper.class);
			job.setReducerClass(PlayerCountReducer.class);
			
			//set inputformat and output format
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			//set the output key and value class
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			//set the input and output paths
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			Path outDir = new Path(args[1]);
			 FileSystem fs = FileSystem.get(job.getConfiguration());
		        if(fs.exists(outDir)){
		            fs.delete(outDir, true);
		        }
			boolean result=job.waitForCompletion(true);
	        if(result)
	        {
	            Job job1 = Job.getInstance();
	            job1.setJarByClass(Driver.class);
	            job1.setMapperClass(PlayerMapper.class);
	            job1.setReducerClass(PlayerReducer.class);
		        job1.setMapOutputKeyClass(Text.class);
		        job1.setMapOutputValueClass(IntWritable.class);

		        job1.setOutputKeyClass(Text.class);
		        job1.setOutputValueClass(IntWritable.class);
	            TextInputFormat.addInputPath(job1, new Path(args[1]));
	            TextOutputFormat.setOutputPath(job1, new Path(args[2]));
	            Path outDir2 = new Path(args[2]);
	   		 FileSystem fs2 = FileSystem.get(job1.getConfiguration());
	   	        if(fs2.exists(outDir2)){
	   	            fs2.delete(outDir2, true);
	   	        }
	            job1.waitForCompletion(true);
	        }
		
		
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
