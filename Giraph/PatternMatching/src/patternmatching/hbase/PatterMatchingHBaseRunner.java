
package patternmatching.hbase;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;

import utils.HBaseRunner;

public class PatterMatchingHBaseRunner extends HBaseRunner {

	public PatterMatchingHBaseRunner() {
		super();
	}

	@Override
	protected String getJobName() {
		return "HBase Pattern Matching";
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
							.forName("patternmatching.hbase.PatternMatchingHBaseInputFormat"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}

	@Override
	protected void setVertexOutputFormatClass(GiraphConfiguration gConf) {
		try {
			gConf
					.setVertexOutputFormatClass((Class<? extends VertexOutputFormat>) Class
							.forName("patternmatching.hbase.PatterMatchingHBaseOutPutFormat"));
		} catch (ClassNotFoundException e) {
			System.exit(-1);
		}
	}
}
