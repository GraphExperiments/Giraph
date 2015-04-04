
package pagerank.hbase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

public class PageRankHBaseInputFormat extends
		HBaseVertexInputFormat<LongWritable, DoubleWritable, DoubleWritable>
		implements
		ImmutableClassesGiraphConfigurable<LongWritable, DoubleWritable, DoubleWritable> {

	private ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, DoubleWritable> conf;

	@Override
	public VertexReader<LongWritable, DoubleWritable, DoubleWritable> createVertexReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException {
		return new LongDoubleDoubleHBaseVertexReader(arg0, arg1);
	}

	@Override
	public void checkInputSpecs(Configuration arg0) {

	}

	@Override
	public void setConf(
			ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, DoubleWritable> configuration) {
		this.conf = configuration;
	}

	@Override
	public ImmutableClassesGiraphConfiguration<LongWritable, DoubleWritable, DoubleWritable> getConf() {
		return conf;
	}

	public class LongDoubleDoubleHBaseVertexReader
			extends
			HBaseVertexInputFormat.HBaseVertexReader<LongWritable, DoubleWritable, DoubleWritable> {

		public LongDoubleDoubleHBaseVertexReader(InputSplit split,
				TaskAttemptContext context) throws IOException {
			super(split, context);
		}

		@Override
		public Vertex<LongWritable, DoubleWritable, DoubleWritable> getCurrentVertex()
				throws IOException, InterruptedException {

			Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex = conf
					.createVertex();

			Result row = getRecordReader().getCurrentValue();

			LongWritable vertexId = new LongWritable(Bytes.toLong(row.getRow()));

			DoubleWritable vertexValue = new DoubleWritable(0);

			ByteArrayInputStream byteIn = new ByteArrayInputStream(row.getValue(
					Bytes.toBytes("outEdges"), Bytes.toBytes("col1")));
			ObjectInputStream in = new ObjectInputStream(byteIn);
			Map<Long, Double> outEdges = null;
			try {
				outEdges = (Map<Long, Double>) in.readObject();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			List<Edge<LongWritable, DoubleWritable>> edges = Lists
					.newArrayListWithCapacity(outEdges.size());
			for (Entry<Long, Double> ed : outEdges.entrySet()) {
				edges.add(EdgeFactory.create(new LongWritable(ed.getKey()),
						new DoubleWritable(ed.getValue())));
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
