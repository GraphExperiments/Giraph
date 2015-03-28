
package hive;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.hive.common.HiveParsing;
import org.apache.giraph.hive.input.vertex.SimpleHiveToVertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.input.parser.Records;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.collect.Lists;

import common.PatternMatchingVertexValue;

public class PatternMatchingVertexInputFormat extends
		SimpleHiveToVertex<LongWritable, PatternMatchingVertexValue, Text> {

	@Override
	public void checkInput(HiveInputDescription inputDesc, HiveTableSchema schema) {
		Records.verifyType(0, HiveType.LONG, schema);
		Records.verifyType(1, HiveType.STRING, schema);
		Records.verifyType(2, HiveType.MAP, schema);
	}

	@Override
	public Iterable<Edge<LongWritable, Text>> getEdges(HiveReadableRecord record) {
		Map<Long, String> hiveEdges = record.getMap(2);
		List<Edge<LongWritable, Text>> edges = Lists
				.newArrayListWithCapacity(hiveEdges.size());
		for (Entry<Long, String> edge : hiveEdges.entrySet()) {
			edges.add(EdgeFactory.create(new LongWritable(edge.getKey()), new Text(
					edge.getValue())));
		}
		return edges;
	}

	@Override
	public LongWritable getVertexId(HiveReadableRecord record) {
		return HiveParsing.parseLongID(record, 0, getReusableVertexId());
	}

	@Override
	public PatternMatchingVertexValue getVertexValue(HiveReadableRecord record) {
		return new PatternMatchingVertexValue(record.getString(1));
	}
}
