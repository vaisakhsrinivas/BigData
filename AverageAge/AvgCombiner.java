package averageage;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class AvgCombiner extends Reducer<LongWritable, FloatWritable, LongWritable, FloatWritable>{
	
	private FloatWritable avgAge = new FloatWritable();
	
	public void reduce(LongWritable key, Iterable<FloatWritable> ages, Context context) throws IOException, InterruptedException{
		float total = 0;
		int count = 0;
		float average = 0;
		for(FloatWritable age : ages){
			total += age.get();			
			count++;
		}
		average = total/count;
		avgAge.set(average);
		context.write(key, avgAge);
	}

}