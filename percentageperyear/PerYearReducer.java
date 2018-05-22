package percentageperyear;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class PerYearReducer extends Reducer<IntWritable, CompositeKeyWritable, IntWritable, CompositeKeyWritable> {

	@Override
	public void reduce(IntWritable key, Iterable<CompositeKeyWritable> values, Context context)
			throws IOException, InterruptedException {

		float w = 0;
		float l = 0;
		float t = 0;
		//float winpercentage = 0;
		//float losspercentage = 0;

		for (CompositeKeyWritable val : values) {
			w += val.getWin();
			l += val.getLoss();
			
		}
		t = t+w+l;
		
		CompositeKeyWritable ckw = new CompositeKeyWritable();
		ckw.setWin(w);
		ckw.setLoss(l);
		ckw.setTotal(t);
		
		//winpercentage = w / t;
		//losspercentage = l /t;
		
		ckw.setPercentage((w/t)*100);
		ckw.setLpercentage((l/t)*100);

		context.write(key, ckw);

	}
}
