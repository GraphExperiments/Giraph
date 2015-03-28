
package patternmatching.hbase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.hbase.HBaseVertexInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

import common.PatternMatchingVertexValue;

public class PatternMatchingHBaseInputFormat extends
		HBaseVertexInputFormat<LongWritable, PatternMatchingVertexValue, Text>
		implements
		ImmutableClassesGiraphConfigurable<LongWritable, PatternMatchingVertexValue, Text> {

	private ImmutableClassesGiraphConfiguration<LongWritable, PatternMatchingVertexValue, Text> conf;

	@Override
	public VertexReader<LongWritable, PatternMatchingVertexValue, Text> createVertexReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException {
		return new LongDoubleDoubleHBaseVertexReader(arg0, arg1);
	}

	@Override
	public void checkInputSpecs(Configuration arg0) {

	}

	@Override
	public void setConf(
			ImmutableClassesGiraphConfiguration<LongWritable, PatternMatchingVertexValue, Text> configuration) {
		this.conf = configuration;
	}

	@Override
	public ImmutableClassesGiraphConfiguration<LongWritable, PatternMatchingVertexValue, Text> getConf() {
		return conf;
	}

	public class LongDoubleDoubleHBaseVertexReader
			extends
			HBaseVertexInputFormat.HBaseVertexReader<LongWritable, PatternMatchingVertexValue, Text> {

		public LongDoubleDoubleHBaseVertexReader(InputSplit split,
				TaskAttemptContext context) throws IOException {
			super(split, context);
		}

		@Override
		public Vertex<LongWritable, PatternMatchingVertexValue, Text> getCurrentVertex()
				throws IOException, InterruptedException {

			Vertex<LongWritable, PatternMatchingVertexValue, Text> vertex = conf
					.createVertex();

			Result row = getRecordReader().getCurrentValue();

			String sID = StringUtils.split(Bytes.toString(row.getRow()), ":")[0];
			String sLabel = StringUtils.split(Bytes.toString(row.getRow()), ":")[1];

			LongWritable vertexId = new LongWritable(Long.parseLong(sID));

			PatternMatchingVertexValue vertexValue = new PatternMatchingVertexValue(
					sLabel);

			ByteArrayInputStream byteIn = new ByteArrayInputStream(row.getValue(
					Bytes.toBytes("outEdges"), Bytes.toBytes("col1")));
			ObjectInputStream in = new ObjectInputStream(byteIn);
			Map<Long, String> outEdges = null;
			try {
				outEdges = (Map<Long, String>) in.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			List<Edge<LongWritable, Text>> edges = Lists
					.newArrayListWithCapacity(outEdges.size());
			for (Entry<Long, String> ed : outEdges.entrySet()) {
				edges.add(EdgeFactory.create(new LongWritable(ed.getKey()),
						new Text(ed.getValue())));
			}
			vertex.initialize(vertexId, vertexValue, edges);
			return vertex;
		}

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}
	}
}
