
package pagerank.hive;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.WorkerContext;

import utils.HiveRunner;

public class PageRankHiveRunner extends HiveRunner {

	public PageRankHiveRunner() {
		super();
	}

	@Override
	protected void setMasterComputeClass(GiraphConfiguration gConf) {
		try {
			gConf.setMasterComputeClass((Class<? extends MasterCompute>) Class
					.forName("common.PageRankMasterCompute"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}

	@Override
	protected void setWorkerContextClass(GiraphConfiguration gConf) {
		try {
			gConf.setWorkerContextClass((Class<? extends WorkerContext>) Class
					.forName("common.PageRankWorkerContext"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}
}
