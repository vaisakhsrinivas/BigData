
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package grassvsclay;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CompositeKeyWritable implements Writable {

	private float win;
	private float loss;
	private float total;
	private float percentage;
	private float lpercentage;

	public CompositeKeyWritable() {
		super();
	}

	public CompositeKeyWritable(float w1, float l1, float t1, float wp, float lp) {
		this.win = w1;
		this.loss = l1;
		this.total = t1;
		this.percentage = wp;
		this.lpercentage = lp;
	}

	public float getWin() {
		return win;
	}

	public void setWin(float win) {
		this.win = win;
	}

	public float getLoss() {
		return loss;
	}

	public void setLoss(float loss) {
		this.loss = loss;
	}

	public float getTotal() {
		return total;
	}

	public void setTotal(float total) {
		this.total = total;
	}

	public void write(DataOutput d) throws IOException {
		d.writeFloat(win);
		d.writeFloat(loss);
		d.writeFloat(total);

	}

	public void readFields(DataInput di) throws IOException {
		win = di.readFloat();
		loss = di.readFloat();
		total = di.readFloat();

	}

	public CompositeKeyWritable read(DataInput in) throws IOException {
		CompositeKeyWritable w = new CompositeKeyWritable();
		w.readFields(in);
		return w;
	}

	public String toString() {
		return (new StringBuilder().append(win).append("\t").append(loss).append("\t").append(total).append("\t")
				.append(percentage).append("\t").append(lpercentage).toString());

	}

	public int compareTo(CompositeKeyWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}

	public float getPercentage() {
		return percentage;
	}

	public void setPercentage(float percentage) {
		this.percentage = percentage;
	}

	public float getLpercentage() {
		return lpercentage;
	}

	public void setLpercentage(float lpercentage) {
		this.lpercentage = lpercentage;
	}

}
