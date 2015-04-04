
package pagerank.hbase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.io.hbase.HBaseVertexOutputFormat;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class PageRankHBaseOutputFormat extends
		HBaseVertexOutputFormat<LongWritable, DoubleWritable, NullWritable>
		implements
		ImmutableClassesGiraphConfigurable<LongWritable, DoubleWritable, NullWritable> {

	private ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, NullWritable> conf;

	@Override
	public VertexWriter<LongWritable, DoubleWritable, NullWritable> createVertexWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new LongDoubleDoubleHBaseVertexWriter(context);
	}

	@Override
	public void setConf(
			ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, NullWritable> conf) {
		this.conf = conf;
	}

	@Override
	public ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, NullWritable> getConf() {
		return conf;
	}

	public class LongDoubleDoubleHBaseVertexWriter
			extends
			HBaseVertexOutputFormat.HBaseVertexWriter<LongWritable, DoubleWritable, NullWritable> {

		public LongDoubleDoubleHBaseVertexWriter(TaskAttemptContext context)
				throws IOException, InterruptedException {
			super(context);
		}

		@Override
		public void writeVertex(
				Vertex<LongWritable, DoubleWritable, NullWritable> vertex)
				throws IOException, InterruptedException {

			System.out.println(String.format("Start Write: %s",
					String.valueOf(System.currentTimeMillis())));

			RecordWriter<ImmutableBytesWritable, Writable> writer = getRecordWriter();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(baos);
			dos.writeLong(vertex.getId().get());
			dos.close();

			ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
			DataOutputStream dos2 = new DataOutputStream(baos2);
			dos2.writeDouble(vertex.getValue().get());
			dos2.close();

			byte[] rowBytes = baos.toByteArray();
			Put put = new Put(rowBytes);
			put.add(Bytes.toBytes("value"), Bytes.toBytes("col1"),
					baos2.toByteArray());
			writer.write(new ImmutableBytesWritable(rowBytes), put);

			System.out.println(String.format("Finish Write: %s",
					String.valueOf(System.currentTimeMillis())));
		}
	}
}
