
package common;

import java.io.IOException;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

public class ShortestPathsComputation
		extends
		BasicComputation<LongWritable, DoubleWritable, DoubleWritable, DoubleWritable> {

	/** The shortest paths id */
	public static final LongConfOption SOURCE_ID = new LongConfOption(
			"SimpleShortestPathsVertex.sourceId", 101, "The shortest paths id");
	/** Class logger */
	private static final Logger LOG = Logger
			.getLogger(ShortestPathsComputation.class);

	/**
	 * Is this vertex the source id?
	 * 
	 * @param vertex
	 *          Vertex
	 * @return True if the source id
	 */
	private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
		return vertex.getId().get() == SOURCE_ID.get(getConf());
	}

	@Override
	public void compute(
			Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex,
			Iterable<DoubleWritable> messages) throws IOException {

		if (getSuperstep() == 0) {
			vertex.setValue(new DoubleWritable(Double.MAX_VALUE));
		}
		double minDist = isSource(vertex) ? 0d : Double.MAX_VALUE;
		for (DoubleWritable message : messages) {
			minDist = Math.min(minDist, message.get());
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Vertex " + vertex.getId() + " got minDist = " + minDist
					+ " vertex value = " + vertex.getValue());
		}
		if (minDist < vertex.getValue().get()) {
			vertex.setValue(new DoubleWritable(minDist));
			for (Edge<LongWritable, DoubleWritable> edge : vertex.getEdges()) {
				double distance = minDist + edge.getValue().get();
				if (LOG.isDebugEnabled()) {
					LOG.debug("Vertex " + vertex.getId() + " sent to "
							+ edge.getTargetVertexId() + " = " + distance);
				}
				sendMessage(edge.getTargetVertexId(), new DoubleWritable(distance));
			}
		}
		vertex.voteToHalt();
	}
}
