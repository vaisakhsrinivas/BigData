package grassvsclay;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PerSurfaceReducer extends Reducer<Text, CompositeKeyWritable, Text, CompositeKeyWritable> {

	CompositeKeyWritable ckw = new CompositeKeyWritable();

	@Override
	public void reduce(Text key, Iterable<CompositeKeyWritable> values, Context context)
			throws IOException, InterruptedException {

		float w = 0;
		float l = 0;
		float t = 0;

		for (CompositeKeyWritable val : values) {
			w += val.getWin();
			l += val.getLoss();
		}
		t = w + l;

		ckw.setWin(w);
		ckw.setLoss(l);
		ckw.setTotal(t);

		ckw.setPercentage((w / t) * 100);
		ckw.setLpercentage((l / t) * 100);

		context.write(key, ckw);
	}
}
