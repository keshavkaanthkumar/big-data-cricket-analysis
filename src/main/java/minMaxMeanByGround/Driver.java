package minMaxMeanByGround;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Summarization Pattern");
        job.setJarByClass(Driver.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MinMaxTuple.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CustomOutputTuple.class);
        job.setMapperClass(GroundMapper.class);
        job.setReducerClass(GroundReducer.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        Path outDir = new Path(args[1]);
        TextOutputFormat.setOutputPath(job, outDir);
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if(fs.exists(outDir)){
            fs.delete(outDir, true);
        }
        job.waitForCompletion(true);
    }
}


