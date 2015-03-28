package shortestpath.hive;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraph.hive.common.HiveParsing;
import org.apache.giraph.hive.input.edge.SimpleHiveToEdge;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.input.parser.Records;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

public class ShortestPathHiveEdgeInputFormat extends
		SimpleHiveToEdge<LongWritable, DoubleWritable> {

	private static Log LOG = LogFactory.getLog(ShortestPathHiveEdgeInputFormat.class);

	@Override
	public void checkInput(HiveInputDescription inputDesc,
			HiveTableSchema schema) {
		Records.verifyType(0, HiveType.LONG, schema);
		Records.verifyType(1, HiveType.LONG, schema);
		Records.verifyType(2, HiveType.DOUBLE, schema);

	}

	@Override
	public DoubleWritable getEdgeValue(HiveReadableRecord hiveRecord) {
		return HiveParsing.parseDoubleWritable(hiveRecord, 2,
				getReusableEdgeValue());
	}

	@Override
	public LongWritable getSourceVertexId(HiveReadableRecord hiveRecord) {
		return HiveParsing.parseLongID(hiveRecord, 0,
				getReusableSourceVertexId());
	}

	@Override
	public LongWritable getTargetVertexId(HiveReadableRecord hiveRecord) {
		return HiveParsing.parseLongID(hiveRecord, 1,
				getReusableTargetVertexId());
	}

}
