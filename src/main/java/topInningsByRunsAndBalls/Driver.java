package topInningsByRunsAndBalls;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"secondary sorting");
        job.setJarByClass(Driver.class);
        job.setGroupingComparatorClass(SortGroupComparator.class);
        job.setSortComparatorClass(SortComparator.class);
        job.setPartitionerClass(KeyPartitioner.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        Path outdir = new Path(args[1]);
        TextOutputFormat.setOutputPath(job, outdir);
        job.setMapperClass(PlayerMapper.class);
        job.setReducerClass(PlayerReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(CompositeKey.class);
        job.setOutputValueClass(Text.class);
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if(fs.exists(outdir))
        {
            fs.delete(outdir, true);
        }
        job.waitForCompletion(true);
    }
}
