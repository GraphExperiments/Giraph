
package shortestpath.hive;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.hive.common.HiveParsing;
import org.apache.giraph.hive.input.vertex.SimpleHiveToVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.input.parser.Records;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

public class ShortestPathHiveVertexInputFormat extends
		SimpleHiveToVertex<LongWritable, DoubleWritable, DoubleWritable> {

	public static Log LOG = LogFactory
			.getLog(ShortestPathHiveVertexInputFormat.class);

	@Override
	public void checkInput(HiveInputDescription inputDesc, HiveTableSchema schema) {
		Records.verifyType(0, HiveType.LONG, schema);
		Records.verifyType(1, HiveType.DOUBLE, schema);
		Records.verifyType(2, HiveType.MAP, schema);
	}

	@Override
	public Iterable<Edge<LongWritable, DoubleWritable>> getEdges(
			HiveReadableRecord record) {
		Iterable<Edge<LongWritable, DoubleWritable>> edg = HiveParsing
				.parseLongDoubleEdges(record, 2);
		return edg;
	}

	@Override
	public LongWritable getVertexId(HiveReadableRecord record) {
		return HiveParsing.parseLongID(record, 0, getReusableVertexId());
	}

	@Override
	public DoubleWritable getVertexValue(HiveReadableRecord record) {
		return HiveParsing.parseDoubleWritable(record, 1, getReusableVertexValue());
	}
}
