package grassvsclay;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PercentagePerSurface extends Mapper<Object, Text, Text, CompositeKeyWritable> {

	private Text Surface = new Text();
	CompositeKeyWritable cw = new CompositeKeyWritable();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		if (value.toString().contains("tourney_id")) {
			return;
		}

		String[] rfarray = value.toString().split(",");
		Surface.set(String.valueOf(rfarray[2]));

		if (rfarray[10].equalsIgnoreCase("Roger Federer")) {
			cw.setWin(1);
			cw.setLoss(0);
		}
		else {
			cw.setLoss(1);
			cw.setWin(0);
		}
		context.write(Surface, cw);
	}

}
