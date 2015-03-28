
package shortestpath.hdfs;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;

import utils.HdfsRunner;

public class HdfsShortestPathRunner extends HdfsRunner {

	public HdfsShortestPathRunner() {
		super();
	}
	
	@Override
	protected String getJobName() {
		return "Hdfs Shortest Path";
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void setComputationClass(GiraphConfiguration gConf) {
		try {
			gConf.setComputationClass((Class<? extends Computation>) Class
					.forName("common.ShortestPathsComputation"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	protected void setVertexInputFormatClass(GiraphConfiguration gConf) {
		try {
			gConf
					.setVertexInputFormatClass((Class<? extends VertexInputFormat>) Class
							.forName("shortestpath.hdfs.ShortestPathHdfsInputFormat"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	protected void setVertexOutputFormatClass(GiraphConfiguration gConf) {
		try {
			gConf
					.setVertexOutputFormatClass((Class<? extends VertexOutputFormat>) Class
							.forName("shortestpath.hdfs.ShortestPathHdfsOutputFormat"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}
}
