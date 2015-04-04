
package common;

import java.io.IOException;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

public class PageRankComputation
		extends
		BasicComputation<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> {

	/** Number of supersteps for this test */
	public static final int MAX_SUPERSTEPS = 21;
	/** Logger */
	private static final Logger LOG = Logger.getLogger(PageRankComputation.class);
	/** Sum aggregator name */
	public static String SUM_AGG = "sum";
	/** Min aggregator name */
	public static String MIN_AGG = "min";
	/** Max aggregator name */
	public static String MAX_AGG = "max";

	@Override
	public void compute(
			Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
			Iterable<DoubleWritable> messages) throws IOException {
		if (getSuperstep() >= 1) {
			double sum = 0;
			for (DoubleWritable message : messages) {
				sum += message.get();
			}
			DoubleWritable vertexValue = new DoubleWritable(
					(0.15f / getTotalNumVertices()) + 0.85f * sum);
			vertex.setValue(vertexValue);
			aggregate(MAX_AGG, vertexValue);
			aggregate(MIN_AGG, vertexValue);
			aggregate(SUM_AGG, new LongWritable(1));
			LOG.info(vertex.getId() + ": PageRank=" + vertexValue + " max="
					+ getAggregatedValue(MAX_AGG) + " min=" + getAggregatedValue(MIN_AGG));
			LOG.info("Number of edges: " + String.valueOf(vertex.getNumEdges()));
		}

		if (getSuperstep() < MAX_SUPERSTEPS) {
			long edges = vertex.getNumEdges();
			sendMessageToAllEdges(vertex, new DoubleWritable(vertex.getValue().get()
					/ edges));
		} else {
			vertex.voteToHalt();
		}
	}
}
