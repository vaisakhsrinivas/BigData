package percentageperyear;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PercentageJob {

	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Top10");
		job.setJarByClass(PercentageJob.class);
		job.setMapperClass(PercentagePerYear.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(CompositeKeyWritable.class);
		job.setReducerClass(PerYearReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(CompositeKeyWritable.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
