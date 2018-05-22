package surface;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Project_Mapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text surface = new Text();
	private IntWritable one = new IntWritable(1);

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException

	{
		if (value.toString().contains("tourney_id"))
			return;

		String[] array = value.toString().split(",");		
		surface.set(array[2]);
		context.write(surface, one);
	}

}
