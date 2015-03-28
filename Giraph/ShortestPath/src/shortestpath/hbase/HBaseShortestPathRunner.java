
package shortestpath.hbase;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;

import utils.HBaseRunner;

public class HBaseShortestPathRunner extends HBaseRunner {

	public HBaseShortestPathRunner() {
		super();
	}

	@Override
	protected String getJobName() {
		return "HBase Shortest Path";
	}

	@Override
	protected void setComputationClass(GiraphConfiguration gConf) {
		try {
			gConf.setComputationClass((Class<? extends Computation>) Class
					.forName("common.ShortestPathsComputation"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}

	@Override
	protected void setVertexInputFormatClass(GiraphConfiguration gConf) {
		try {
			gConf
					.setVertexInputFormatClass((Class<? extends VertexInputFormat>) Class
							.forName("shortestpath.hbase.ShortestPathHBaseInputFormat"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}

	@Override
	protected void setVertexOutputFormatClass(GiraphConfiguration gConf) {
		try {
			gConf
					.setVertexOutputFormatClass((Class<? extends VertexOutputFormat>) Class
							.forName("shortestpath.hbase.ShortestPathHBaseOutputFormat"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}
}
