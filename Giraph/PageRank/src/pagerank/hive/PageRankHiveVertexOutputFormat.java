
package pagerank.hive;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.hive.output.SimpleVertexToHive;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.output.HiveOutputDescription;
import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.base.Preconditions;

public class PageRankHiveVertexOutputFormat extends
		SimpleVertexToHive<LongWritable, DoubleWritable, NullWritable> {

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
		System.out.println("Start Write : "
				+ String.valueOf(System.currentTimeMillis()));
		record.setLong(0, (long) vertex.getId().get());
		record.setDouble(1, (double) vertex.getValue().get());
		System.out.println("Finish Write : "
				+ String.valueOf(System.currentTimeMillis()));
	}
}
