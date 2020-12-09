package topNInningsScores;

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
			
			   Job topNInningsJob = Job.getInstance(config, "Top N Innings using comparator");
			   topNInningsJob.setJarByClass(Driver.class);
	            int N = 10;
	            topNInningsJob.getConfiguration().setInt("N", N);
	            topNInningsJob.setInputFormatClass(TextInputFormat.class);
	            topNInningsJob.setOutputFormatClass(TextOutputFormat.class);

	            topNInningsJob.setMapperClass(PlayerMapper.class);
	            topNInningsJob.setSortComparatorClass(Comparator.class);
	            topNInningsJob.setReducerClass(PlayerReducer.class);
	            topNInningsJob.setNumReduceTasks(1);

	            topNInningsJob.setMapOutputKeyClass(IntWritable.class);
	            topNInningsJob.setMapOutputValueClass(Text.class);
	            topNInningsJob.setOutputKeyClass(IntWritable.class);
	            topNInningsJob.setOutputValueClass(Text.class);

	            FileInputFormat.setInputPaths(topNInningsJob, new Path(args[0]));  
	            FileOutputFormat.setOutputPath(topNInningsJob, new Path(args[1]));
	            Path outDir = new Path(args[1]);
	   		 FileSystem fs = FileSystem.get(topNInningsJob.getConfiguration());
	            if (fs.exists(new Path(args[1]))) {
	                fs.delete(new Path(args[1]), true);
	            }
			System.exit(topNInningsJob.waitForCompletion(true)?0:1);
			
			
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
