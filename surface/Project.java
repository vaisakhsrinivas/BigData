package surface;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Project {

	public static void main(String[] args) throws IOException,InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "surface");
		job.setJarByClass(Project.class);
		job.setMapperClass(Project_Mapper.class);
		job.setCombinerClass(Project_Reducer.class);
		job.setReducerClass(Project_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0:1);
	}

}
