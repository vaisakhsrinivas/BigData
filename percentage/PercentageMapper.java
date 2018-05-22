package percentage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PercentageMapper extends Mapper<Object, Text, Text, FloatWritable> {

	HashMap<String, Integer> map = new HashMap<String, Integer>();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] rfarray = value.toString().split(",");

		if (rfarray[10].equalsIgnoreCase("Roger Federer")) {
			if (!map.containsKey("win")) {
				map.put("win", 1);
			} else {
				map.put("win", map.get("win") + 1);
			}

		} else {
			if (!map.containsKey("lost")) {
				map.put("lost", 1);
			} else {
				map.put("lost", map.get("lost") + 1);
			}

		}

	}

	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		float w;
		float l;
		float t;

		w = map.get("win");
		l = map.get("lost");

		t = w + l;

		float wp = (w / t) * 100;
		float lp = (l / t) * 100;

		context.write(new Text("win %"), new FloatWritable(wp));

		context.write(new Text("lost %"), new FloatWritable(lp));

	}

}
