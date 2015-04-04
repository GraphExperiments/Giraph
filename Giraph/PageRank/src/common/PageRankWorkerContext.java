
package common;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

public class PageRankWorkerContext extends WorkerContext {

	/** Logger */
	private static final Logger LOG = Logger
			.getLogger(PageRankWorkerContext.class);

	/** Final max value for verification for local jobs */
	private static double FINAL_MAX;
	/** Final min value for verification for local jobs */
	private static double FINAL_MIN;
	/** Final sum value for verification for local jobs */
	private static long FINAL_SUM;

	public static double getFinalMax() {
		return FINAL_MAX;
	}

	public static double getFinalMin() {
		return FINAL_MIN;
	}

	public static long getFinalSum() {
		return FINAL_SUM;
	}

	@Override
	public void preApplication() throws InstantiationException,
			IllegalAccessException {}

	@Override
	public void postApplication() {
		FINAL_SUM = this.<LongWritable> getAggregatedValue(
				PageRankComputation.SUM_AGG).get();
		FINAL_MAX = this.<DoubleWritable> getAggregatedValue(
				PageRankComputation.MAX_AGG).get();
		FINAL_MIN = this.<DoubleWritable> getAggregatedValue(
				PageRankComputation.MIN_AGG).get();

		LOG.info("aggregatedNumVertices=" + FINAL_SUM);
		LOG.info("aggregatedMaxPageRank=" + FINAL_MAX);
		LOG.info("aggregatedMinPageRank=" + FINAL_MIN);
	}

	@Override
	public void preSuperstep() {
		if (getSuperstep() >= 3) {
			LOG.info("aggregatedNumVertices="
					+ getAggregatedValue(PageRankComputation.SUM_AGG) + " NumVertices="
					+ getTotalNumVertices());
			if (this.<LongWritable> getAggregatedValue(PageRankComputation.SUM_AGG)
					.get() != getTotalNumVertices()) {
				throw new RuntimeException("wrong value of SumAggreg: "
						+ getAggregatedValue(PageRankComputation.SUM_AGG) + ", should be: "
						+ getTotalNumVertices());
			}
			DoubleWritable maxPagerank = getAggregatedValue(PageRankComputation.MAX_AGG);
			LOG.info("aggregatedMaxPageRank=" + maxPagerank.get());
			DoubleWritable minPagerank = getAggregatedValue(PageRankComputation.MIN_AGG);
			LOG.info("aggregatedMinPageRank=" + minPagerank.get());
		}
	}

	@Override
	public void postSuperstep() {}

}
