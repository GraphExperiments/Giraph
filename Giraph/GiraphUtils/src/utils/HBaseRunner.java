
package utils;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.util.Tool;

public abstract class HBaseRunner implements Tool {

	static {
		Configuration.addDefaultResource("giraph-site.xml");
	}

	/** Writable conf */
	protected Configuration conf;

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		if (null == getConf()) { // for YARN profile
			conf = HBaseConfiguration.create(new Configuration());
		}
		conf.set("hbase.zookeeper.quorum", "master");
		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());

		final int workers = Integer.parseInt(args[1]);

		giraphConf.setWorkerConfiguration(workers, workers, 100.0f);

		setWorkerContextClass(giraphConf);
		setMasterComputeClass(giraphConf);
		setVertexInputFormatClass(giraphConf);
		setVertexOutputFormatClass(giraphConf);
		setComputationClass(giraphConf);

		giraphConf.setZooKeeperConfiguration("master:2181");

		giraphConf.set(TableInputFormat.INPUT_TABLE, args[2]);
		giraphConf.set(TableOutputFormat.OUTPUT_TABLE, args[3]);

		GiraphJob job = new GiraphJob(giraphConf, getJobName());

		boolean verbose = true;
		return job.run(verbose) ? 0 : -1;
	}

	protected abstract String getJobName();

	protected void setVertexInputFormatClass(GiraphConfiguration gConf) {}

	protected void setVertexOutputFormatClass(GiraphConfiguration gConf) {}

	protected abstract void setComputationClass(GiraphConfiguration gConf);

	protected void setWorkerContextClass(GiraphConfiguration gConf) {}

	protected void setMasterComputeClass(GiraphConfiguration gConf) {}

}
