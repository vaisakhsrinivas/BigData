package headTohead;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HeadToHeadReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	public void reduce(IntWritable year, Iterable<IntWritable> counts, Context context)
			throws IOException, InterruptedException {

		int totalCount = 0;
		for (IntWritable count : counts) {
			totalCount += count.get();
		}
		context.write(year, new IntWritable(totalCount));
	}

}
