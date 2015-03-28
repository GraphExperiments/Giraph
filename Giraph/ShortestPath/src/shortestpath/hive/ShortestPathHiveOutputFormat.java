
package shortestpath.hive;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.hive.output.SimpleVertexToHive;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.output.HiveOutputDescription;
import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.base.Preconditions;

public class ShortestPathHiveOutputFormat extends
		SimpleVertexToHive<LongWritable, DoubleWritable, NullWritable> {

	private Log LOG = LogFactory.getLog(ShortestPathHiveOutputFormat.class);

	@Override
	public void checkOutput(HiveOutputDescription outputDesc,
			HiveTableSchema schema, HiveWritableRecord emptyRecord) {
		Preconditions.checkArgument(schema.columnType(0) == HiveType.LONG);
		Preconditions.checkArgument(schema.columnType(1) == HiveType.DOUBLE);

	}

	@Override
	public void fillRecord(
			Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
			HiveWritableRecord record) {
		System.out.println("Start Write : " + String.valueOf(new Date().getTime()));
		record.setLong(0, (long) vertex.getId().get());
		record.setDouble(1, (double) vertex.getValue().get());
		System.out
				.println("Finish Write : " + String.valueOf(new Date().getTime()));
	}

}
