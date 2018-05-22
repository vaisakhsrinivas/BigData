package headTohead;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class HeadToHeadMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

	private IntWritable one = new IntWritable(1);
	private IntWritable year = new IntWritable();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		if (value.toString().contains("tourney_id")) {
			return;
		}

		String[] rfarray = value.toString().split(",");
		String[] date = rfarray[0].split("-");
		year.set(Integer.parseInt(date[0]));
		
		if(rfarray[10].equalsIgnoreCase("Roger Federer")) {
			if(rfarray[20].equalsIgnoreCase("Rafael Nadal")) {
				context.write(year, one);
			}
		}else if(rfarray[10].equalsIgnoreCase("Rafael Nadal")) {
			if(rfarray[20].equalsIgnoreCase("Roger Federer")) {
				context.write(year, one);
			}
		}

		
	}
}
