package tournament;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Project_Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text tournament, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
		
		int totalCount = 0;
		for ( IntWritable count : counts) {
			totalCount += count.get();
		}
		context.write(tournament, new IntWritable(totalCount));
	}

}
