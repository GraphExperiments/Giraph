
package hive;

import org.apache.giraph.hive.common.HiveParsing;
import org.apache.giraph.hive.input.edge.SimpleHiveToEdge;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.input.parser.Records;
import com.facebook.hiveio.record.HiveReadableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;

public class PatternMatchingEdgeInputFormat extends
		SimpleHiveToEdge<LongWritable, Text> {

	@Override
	public void checkInput(HiveInputDescription inputDesc, HiveTableSchema schema) {
		Records.verifyType(0, HiveType.LONG, schema);
		Records.verifyType(1, HiveType.LONG, schema);
		Records.verifyType(2, HiveType.STRING, schema);
	}

	@Override
	public Text getEdgeValue(HiveReadableRecord hiveRecord) {
		return new Text(hiveRecord.getString(2));
	}

	@Override
	public LongWritable getSourceVertexId(HiveReadableRecord hiveRecord) {
		return HiveParsing.parseLongID(hiveRecord, 0, getReusableSourceVertexId());
	}

	@Override
	public LongWritable getTargetVertexId(HiveReadableRecord hiveRecord) {
		return HiveParsing.parseLongID(hiveRecord, 1, getReusableTargetVertexId());
	}

}
