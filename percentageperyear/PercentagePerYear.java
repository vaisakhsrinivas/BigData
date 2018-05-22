package percentageperyear;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PercentagePerYear extends Mapper<Object, Text, IntWritable, CompositeKeyWritable> {

	HashMap<String, Integer> map = new HashMap<String, Integer>();
	private IntWritable year = new IntWritable();
	private IntWritable One = new IntWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		if (value.toString().contains("tourney_id")) {
			return;
		}

		String[] rfarray = value.toString().split(",");
		String[] date = rfarray[0].split("-");
		year.set(Integer.parseInt(date[0]));

		CompositeKeyWritable cw = new CompositeKeyWritable();

		if (rfarray[10].equalsIgnoreCase("Roger Federer"))
			
		{
			cw.setWin(1);
		}
		else
		{
			cw.setLoss(1);
		}
		context.write(year, cw);
	}

}
