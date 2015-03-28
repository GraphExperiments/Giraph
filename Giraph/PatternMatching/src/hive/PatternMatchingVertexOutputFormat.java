
package hive;

import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.hive.output.SimpleVertexToHive;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.output.HiveOutputDescription;
import com.facebook.hiveio.record.HiveWritableRecord;
import com.facebook.hiveio.schema.HiveTableSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import common.PatternMatchingVertexValue;

public class PatternMatchingVertexOutputFormat extends
		SimpleVertexToHive<LongWritable, PatternMatchingVertexValue, Text> {

	@Override
	public void checkOutput(HiveOutputDescription outputDesc,
			HiveTableSchema schema, HiveWritableRecord emptyRecord) {
		Preconditions.checkArgument(schema.columnType(0) == HiveType.LONG);
		Preconditions.checkArgument(schema.columnType(1) == HiveType.LIST);
	}

	@Override
	public void fillRecord(
			Vertex<LongWritable, PatternMatchingVertexValue, Text> vertex,
			HiveWritableRecord record) {
		System.out.println(String.format("Start Write: %s", new Date().getTime()));
		if (vertex.getValue().isMatch()) {
			record.setLong(0, vertex.getId().get());
			List<String> outLables = Lists.newLinkedList();
			for (Entry<Writable, Writable> kv : vertex.getValue().getMatchSet()
					.entrySet()) {
				String lable = ((Text) kv.getValue()).toString();
				outLables.add(lable);
			}
			record.setList(1, outLables);
			System.out
					.println(String.format("Finish Write: %s", new Date().getTime()));
		}
	}
}
