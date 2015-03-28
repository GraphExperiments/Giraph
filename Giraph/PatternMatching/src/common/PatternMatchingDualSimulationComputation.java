
package common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map.Entry;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;

public class PatternMatchingDualSimulationComputation
		extends
		BasicComputation<LongWritable, PatternMatchingVertexValue, Text, PatternMatchingMessage> {

	private static String[] pattern = { "A", "B", "F", "E", "R" };

	private boolean matchWithQuery(String label) {
		return Arrays.asList(pattern).contains(label);
	}

	@Override
	public void compute(
			Vertex<LongWritable, PatternMatchingVertexValue, Text> vertex,
			Iterable<PatternMatchingMessage> messages) throws IOException {
		if (getSuperstep() == 0) {
			PatternMatchingVertexValue newValue = new PatternMatchingVertexValue(
					vertex.getValue());
			if (matchWithQuery(vertex.getValue().getLabel())) {
				newValue.setMatch(true);
				for (Edge<LongWritable, Text> edge : vertex.getEdges()) {
					long targetID = edge.getTargetVertexId().get();
					String targetLabel = edge.getValue().toString();
					newValue.addMatchSet(targetID, targetLabel);
					sendMessage(new LongWritable(targetID), new PatternMatchingMessage(
							vertex.getId().get(), vertex.getValue().getLabel()));

				}
				vertex.setValue(newValue);
			} else {
				newValue.setMatch(false);
				vertex.setValue(newValue);
				vertex.voteToHalt();
			}
		} else if (getSuperstep() == 1) {
			if (!vertex.getValue().isMatch()) {
				vertex.voteToHalt();
			} else {
				PatternMatchingVertexValue newValue = new PatternMatchingVertexValue(
						vertex.getValue());
				for (PatternMatchingMessage msg : messages) {
					long parentID = msg.getSenderID();
					String parentLabel = msg.getSenderLabel();
					newValue.addParent(parentID, parentLabel);
					sendMessage(new LongWritable(parentID), new PatternMatchingMessage(
							vertex.getId().get(), vertex.getValue().getLabel(), vertex
									.getValue().getMatchSet()));
				}

				vertex.setValue(newValue);
			}
		} else if (getSuperstep() == 2) {
			if (vertex.getValue().isMatch()) {
				PatternMatchingVertexValue newValue = new PatternMatchingVertexValue(
						vertex.getValue());
				if (!messages.iterator().hasNext()) {
					newValue.setMatch(false);
					ArrayList<LongWritable> removeList = Lists
							.newArrayListWithExpectedSize(newValue.getMatchSet().keySet()
									.size());
					for (Writable rid : newValue.getMatchSet().keySet()) {
						removeList.add((LongWritable) rid);
					}
					newValue.clearMatchSet();
					for (Entry<Writable, Writable> parentNode : newValue.getParents()
							.entrySet()) {

						long parentID = ((LongWritable) parentNode.getKey()).get();
						sendMessage(new LongWritable(parentID),
								new PatternMatchingMessage(vertex.getId().get(), vertex
										.getValue().getLabel(), newValue.getMatchSet(), removeList));
					}
					vertex.setValue(newValue);
					vertex.voteToHalt();
				} else {
					ArrayList<LongWritable> childrenMatchList = Lists.newArrayList();
					ArrayList<LongWritable> removeList = Lists.newArrayList();
					for (PatternMatchingMessage msg : messages) {
						childrenMatchList.add(new LongWritable(msg.getSenderID()));
					}

					for (Writable mSet : newValue.getMatchSet().keySet()) {
						if (!childrenMatchList.contains((LongWritable) mSet)) {
							removeList.add(new LongWritable(((LongWritable) mSet).get()));
						}
					}

					if (!removeList.isEmpty()) {
						newValue.removeMatchSet(removeList);
						for (Writable parentID : newValue.getParents().keySet()) {
							sendMessage(new LongWritable(((LongWritable) parentID).get()),
									new PatternMatchingMessage(vertex.getId().get(), vertex
											.getValue().getLabel(), newValue.getMatchSet(),
											removeList));
						}
						if (newValue.getMatchSet().isEmpty()) {
							newValue.setMatch(false);
						}
						vertex.setValue(newValue);
					} else {
						vertex.voteToHalt();
					}
				}
			} else {
				vertex.voteToHalt();
			}
		} else if (getSuperstep() >= 3) {
			if (!messages.iterator().hasNext()) {
				vertex.voteToHalt();
			} else {
				ArrayList<LongWritable> removeList = Lists.newArrayList();
				for (PatternMatchingMessage msg : messages) {
					for (LongWritable id : msg.getRemoveList()) {
						if (vertex.getValue().getMatchSet().containsKey(id)) {
							removeList.add((LongWritable) id);
						}
					}
					if (msg.getMatchSet().isEmpty()) {
						removeList.add(new LongWritable(msg.getSenderID()));
					}
				}
				if (!removeList.isEmpty()) {
					PatternMatchingVertexValue newValue = new PatternMatchingVertexValue(
							vertex.getValue());
					newValue.removeMatchSet(removeList);
					if (newValue.getMatchSet().isEmpty()) {
						newValue.setMatch(false);
					}
					for (Writable parentID : newValue.getParents().keySet()) {
						sendMessage((LongWritable) parentID,
								new PatternMatchingMessage(vertex.getId().get(), vertex
										.getValue().getLabel(), newValue.getMatchSet(), removeList));
					}
					vertex.setValue(newValue);
				} else {
					vertex.voteToHalt();
				}
			}
		}
	}
}
