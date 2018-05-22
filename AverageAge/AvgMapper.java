package averageage;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgMapper extends Mapper<Object, Text, LongWritable, FloatWritable> {

	private LongWritable year = new LongWritable();
	private FloatWritable age = new FloatWritable();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		if (value.toString().contains("tourney_id")) {
			return;
		}

		String[] yeararray = value.toString().split(",");

		String[] date = yeararray[0].split("-");
		year.set(Long.parseLong(date[0]));
		age.set(Float.parseFloat(yeararray[14]));
		context.write(year, age);

	}

}
