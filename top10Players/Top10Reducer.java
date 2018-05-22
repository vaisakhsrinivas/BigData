package top10Players;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	static final TreeMap<Float, String> map = new TreeMap<Float, String>() ;
	
	public void reduce(Text key, Iterable<FloatWritable> points, Context context)
			throws IOException, InterruptedException {
		
		float total = 0;
	
		
		for (FloatWritable wpoints : points) {
			total += wpoints.get();
		}
		
		
		if(!map.containsKey(total)) {	
			map.put(total, key.toString());
		}

		if (map.size() > 10) {
			map.remove(map.firstKey());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Float points : map.keySet()) {
			context.write(new Text(map.get(points)), new FloatWritable(points));
		}
	}

}