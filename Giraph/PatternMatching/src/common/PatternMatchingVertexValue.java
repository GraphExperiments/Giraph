
package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PatternMatchingVertexValue implements Writable {

	private Text label;
	private BooleanWritable match;
	private MapWritable matchSet;
	private MapWritable parents;

	public boolean isMatch() {
		return match.get();
	}

	public void setMatch(boolean match) {
		this.match.set(match);
	}

	public void setLabel(String label) {
		this.label = new Text(label);
	}

	public String getLabel() {
		return label.toString();
	}

	public MapWritable getMatchSet() {
		return matchSet;
	}

	public void setMatchSet(MapWritable matchSet) {
		this.matchSet = new MapWritable(matchSet);
	}

	public MapWritable getParents() {
		return parents;
	}

	public void setParents(MapWritable parents) {
		this.parents = new MapWritable(parents);
	}

	public void addMatchSet(long id, String label) {
		matchSet.put(new LongWritable(id), new Text(label));
	}

	public void addParent(long id, String label) {
		parents.put(new LongWritable(id), new Text(label));
	}

	public void removeMatchSet(LongWritable id) {
		matchSet.remove(id);
	}

	public void removeMatchSet(ArrayList<LongWritable> ids) {
		for (LongWritable id : ids)
			removeMatchSet(id);
	}

	public void clearMatchSet() {
		matchSet.clear();
	}

	public PatternMatchingVertexValue() {
		match = new BooleanWritable(false);
		label = new Text();
		matchSet = new MapWritable();
		parents = new MapWritable();
	}

	public PatternMatchingVertexValue(String lable) {
		match = new BooleanWritable(false);
		label = new Text(lable);
		matchSet = new MapWritable();
		parents = new MapWritable();
	}

	public PatternMatchingVertexValue(final PatternMatchingVertexValue vertexValue) {
		label = new Text(vertexValue.getLabel());
		match = new BooleanWritable(vertexValue.isMatch());
		matchSet = new MapWritable(vertexValue.getMatchSet());
		parents = new MapWritable(vertexValue.getParents());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		label.readFields(in);
		match.readFields(in);
		matchSet.readFields(in);
		parents.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		label.write(out);
		match.write(out);
		matchSet.write(out);
		parents.write(out);
	}

	@Override
	public String toString() {
		return "isMatch=" + Boolean.toString(isMatch());
	}
}
