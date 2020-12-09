package noOfCenturiesByGround;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	  Configuration conf = new Configuration();
      Job job = Job.getInstance(conf,"Counter");
      job.setJarByClass(Driver.class);
      job.setMapperClass(GroundMapper.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		Path outDir = new Path(args[1]);
		 FileSystem fs = FileSystem.get(job.getConfiguration());
	        if(fs.exists(outDir)){
	            fs.delete(outDir, true);
	        }
      int code = job.waitForCompletion(true) ? 0 : 1;
      if(code==0)
      {
          for(Counter counter: job.getCounters().getGroup(GroundMapper.GROUND)){
              System.out.println(counter.getDisplayName()+"\t"+counter.getValue());
          }
      }

      System.exit(code);
  }
}
