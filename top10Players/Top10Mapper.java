package top10Players;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Top10Mapper extends Mapper<Object, Text, Text, FloatWritable> {

	private Text winnername = new Text();
	private FloatWritable winnerpoints = new FloatWritable();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		if (value.toString().contains("tourney_id")) {
			return;
		}

		String[] yeararray = value.toString().split(",");

		String name = yeararray[10];
		String points = yeararray[16];
		if (points.equalsIgnoreCase("NA")) {
			return;
		}
		winnername.set(name);
		winnerpoints.set(Float.parseFloat(points));
		context.write(winnername, winnerpoints);

	}

}
