
package patternmatching.hbase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.io.hbase.HBaseVertexOutputFormat;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

import common.PatternMatchingVertexValue;

public class PatterMatchingHBaseOutPutFormat extends
		HBaseVertexOutputFormat<LongWritable, PatternMatchingVertexValue, Text>
		implements
		ImmutableClassesGiraphConfigurable<LongWritable, PatternMatchingVertexValue, Text> {

	private ImmutableClassesGiraphConfiguration<LongWritable, PatternMatchingVertexValue, Text> conf;

	@Override
	public VertexWriter<LongWritable, PatternMatchingVertexValue, Text> createVertexWriter(
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new LongDoubleDoubleHBaseVertexWriter(context);
	}

	@Override
	public void setConf(
			ImmutableClassesGiraphConfiguration<LongWritable, PatternMatchingVertexValue, Text> conf) {
		// TODO Auto-generated method stub
		// conf.set(TableOutputFormat.OUTPUT_TABLE, "outDataPR");
		this.conf = conf;
	}

	@Override
	public ImmutableClassesGiraphConfiguration<LongWritable, PatternMatchingVertexValue, Text> getConf() {
		return conf;
	}

	public class LongDoubleDoubleHBaseVertexWriter
			extends
			HBaseVertexOutputFormat.HBaseVertexWriter<LongWritable, PatternMatchingVertexValue, Text> {

		public LongDoubleDoubleHBaseVertexWriter(TaskAttemptContext context)
				throws IOException, InterruptedException {
			super(context);
		}

		@Override
		public void writeVertex(
				Vertex<LongWritable, PatternMatchingVertexValue, Text> vertex)
				throws IOException, InterruptedException {

			if (!vertex.getValue().getMatchSet().isEmpty()
					&& vertex.getValue().isMatch()) {
				System.out.println("Start Write: "
						+ String.valueOf(new Date().getTime()));
				RecordWriter<ImmutableBytesWritable, Writable> writer = getRecordWriter();
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputStream dos = new DataOutputStream(baos);
				dos.writeLong(vertex.getId().get());
				dos.close();

				List<String> outEdges = Lists.newLinkedList();

				for (Entry<Writable, Writable> idLable : vertex.getValue()
						.getMatchSet().entrySet()) {
					outEdges.add(idLable.getValue().toString());
				}

				ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
				new ObjectOutputStream(baos2).writeObject(outEdges);

				byte[] rowBytes = baos.toByteArray();
				Put put = new Put(rowBytes);
				put.add(Bytes.toBytes("outEdges"), Bytes.toBytes("col1"),
						baos2.toByteArray());
				writer.write(new ImmutableBytesWritable(rowBytes), put);

				System.out.println("Finish Write: "
						+ String.valueOf(new Date().getTime()));
			}
		}
	}
}
