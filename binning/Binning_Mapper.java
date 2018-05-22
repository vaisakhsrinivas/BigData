package binning;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Binning_Mapper extends Mapper<Object, Text, Text, NullWritable> {

	private MultipleOutputs<Text, NullWritable> mos = null;

	protected void setup(Context context) {
		mos = new MultipleOutputs<Text, NullWritable>(context);
	}

	public void map(Object key, Text value, Context context) {
		try {

			if (value.toString().contains("tourney_id"))
				return;

			String[] array = value.toString().split(",");
			String tournament = array[1];

			mos.write("bins", value.toString(), NullWritable.get(), tournament);
		}

		catch (Exception e) {

			System.out.println(e.getMessage());

		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {

		mos.close();

	}

}