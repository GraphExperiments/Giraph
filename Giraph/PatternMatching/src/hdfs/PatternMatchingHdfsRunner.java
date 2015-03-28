
package hdfs;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;

import utils.HdfsRunner;

public class PatternMatchingHdfsRunner extends HdfsRunner {
	
	public PatternMatchingHdfsRunner() {
		super();
	}

	@Override
	protected String getJobName() {
		return "Hdfs Pattern Matching";
	}

	@Override
	protected void setComputationClass(GiraphConfiguration gConf) {
		try {
			gConf.setComputationClass((Class<? extends Computation>) Class
					.forName("common.PatternMatchingDualSimulationComputation"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}

	@Override
	protected void setVertexInputFormatClass(GiraphConfiguration gConf) {
		try {
			gConf
					.setVertexInputFormatClass((Class<? extends VertexInputFormat>) Class
							.forName("hdfs.PatternMatchingInputFormat"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}

	@Override
	protected void setVertexOutputFormatClass(GiraphConfiguration gConf) {
		try {
			gConf
					.setVertexOutputFormatClass((Class<? extends VertexOutputFormat>) Class
							.forName("hdfs.PatternMatchingOutputFormat"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}

}
