
package pagerank.hdfs;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.master.MasterCompute;
import org.apache.giraph.worker.WorkerContext;

import utils.HdfsRunner;

public class PageRankHdfsRunner extends HdfsRunner {

	public PageRankHdfsRunner() {
		super();
	}

	@Override
	protected String getJobName() {
		return "Hdfs Page Rank";
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
	protected void setVertexInputFormatClass(GiraphConfiguration gConf) {
		try {
			gConf
					.setVertexInputFormatClass((Class<? extends VertexInputFormat>) Class
							.forName("pagerank.hdfs.PageRankInputFormat"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}

	@Override
	protected void setVertexOutputFormatClass(GiraphConfiguration gConf) {
		try {
			gConf
					.setVertexOutputFormatClass((Class<? extends VertexOutputFormat>) Class
							.forName("pagerank.hdfs.PageRankVertexOutputFormat"));
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

	@Override
	protected void setMasterComputeClass(GiraphConfiguration gConf) {
		try {
			gConf.setMasterComputeClass((Class<? extends MasterCompute>) Class
					.forName("common.PageRankMasterCompute"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}

}
