
package pagerank.hbase;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.WorkerContext;

import utils.HBaseRunner;

public class PageRankHBaseRunner extends HBaseRunner {

	public PageRankHBaseRunner() {
		super();
	}

	@Override
	protected String getJobName() {
		return "Hbase Page Rank";
	}

	@Override
	protected void setComputationClass(GiraphConfiguration gConf) {
		try {
			gConf.setComputationClass((Class<? extends Computation>) Class
					.forName("common.PageRankComputation"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}

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
	protected void setVertexInputFormatClass(GiraphConfiguration gConf) {
		try {
			gConf.setVertexInputFormatClass((Class<? extends VertexInputFormat>) Class
					.forName("pagerank.hbase.PageRankHBaseInputFormat"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}

	@Override
	protected void setVertexOutputFormatClass(GiraphConfiguration gConf) {
		try {
			gConf.setVertexOutputFormatClass((Class<? extends VertexOutputFormat>) Class
					.forName("pagerank.hbase.PageRankHBaseOutputFormat"));
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
