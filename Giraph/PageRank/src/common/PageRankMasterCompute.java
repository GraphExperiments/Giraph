
package common;

import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.aggregators.DoubleMinAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

public class PageRankMasterCompute extends DefaultMasterCompute {

	@Override
	public void initialize() throws InstantiationException,
			IllegalAccessException {
		registerAggregator(PageRankComputation.SUM_AGG, LongSumAggregator.class);
		registerPersistentAggregator(PageRankComputation.MIN_AGG,
				DoubleMinAggregator.class);
		registerPersistentAggregator(PageRankComputation.MAX_AGG,
				DoubleMaxAggregator.class);
	}

}
