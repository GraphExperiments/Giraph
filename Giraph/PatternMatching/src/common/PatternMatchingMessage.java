package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class PatternMatchingMessage implements Writable {

	private LongWritable senderID;
	private Text senderLabel;
	private MapWritable matchSet;
	private LongWritable[] removeList;

	public long getSenderID() {
		return senderID.get();
	}

	public void setSenderID(long senderID) {
		this.senderID = new LongWritable(senderID);
	}

	public String getSenderLabel() {
		return senderLabel.toString();
	}

	public void setSenderLabel(String senderLabel) {
		this.senderLabel = new Text(senderLabel);
	}

	public void setMatchSet(MapWritable matchSet) {
		this.matchSet = new MapWritable(matchSet);
	}

	public MapWritable getMatchSet() {
		return matchSet;
	}

	public LongWritable[] getRemoveList() {
		return removeList;
	}

	public void setRemoveList(LongWritable[] list) {
		if (list != null) removeList = list.clone();
		else removeList = new LongWritable[0];
	}

	public void serRemoveList(ArrayList<LongWritable> list) {
		if (list.isEmpty()) removeList = new LongWritable[0];
		else removeList = list.toArray(new LongWritable[list.size()]);
	}

	public PatternMatchingMessage(long senderID, String senderLable,
			MapWritable matchSet) {
		this();
		this.senderID.set(senderID);
		this.senderLabel.set(senderLable);
		this.matchSet.putAll(matchSet);
	}

	public PatternMatchingMessage(long senderID, String senderLable,
			MapWritable matchSet, LongWritable[] removeList) {
		this();
		this.senderID.set(senderID);
		this.senderLabel.set(senderLable);
		this.matchSet.putAll(matchSet);
		this.setRemoveList(removeList);
	}

	public PatternMatchingMessage(long senderID, String senderLable,
			MapWritable matchSet, ArrayList<LongWritable> removeList) {
		this();
		this.senderID.set(senderID);
		this.senderLabel.set(senderLable);
		this.matchSet.putAll(matchSet);
		this.serRemoveList(removeList);
	}

	public PatternMatchingMessage() {
		senderID = new LongWritable();
		senderLabel = new Text();
		matchSet = new MapWritable();
		removeList = new LongWritable[0];
	}

	public PatternMatchingMessage(long senderID, String senderLable) {
		this();
		this.senderID.set(senderID);
		this.senderLabel.set(senderLable);
	}

	public PatternMatchingMessage(LongWritable senderID, Text senderLable) {
		this();
		this.senderID.set(senderID.get());
		this.senderLabel.set(senderLable);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		senderID.readFields(in);
		senderLabel.readFields(in);
		matchSet.readFields(in);
		int removeListLenght = in.readInt();
		removeList = new LongWritable[removeListLenght];
		for (int i = 0; i < removeListLenght; i++) {
			LongWritable rVertex = new LongWritable();
			rVertex.readFields(in);
			removeList[i] = rVertex;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		senderID.write(out);
		senderLabel.write(out);
		matchSet.write(out);
		int removeListLenght = 0;
		if (removeList != null) {
			removeListLenght = removeList.length;
		}
		out.writeInt(removeListLenght);
		for (int i = 0; i < removeListLenght; i++) {
			removeList[i].write(out);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder(String.format("Sender= %s \n",
				this.senderID.get()));
		if (!matchSet.isEmpty()) {
			sb.append("MatchSet : ");
			for (Entry<Writable, Writable> n : matchSet.entrySet()) {
				sb.append(n.getKey() + ",");
			}
		}
		return sb.toString();
	}
}
