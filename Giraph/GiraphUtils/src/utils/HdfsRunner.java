
package utils;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public abstract class HdfsRunner implements Tool {

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
	/**
	 * Drives a job run configured for "Giraph on Hadoop MR cluster"
	 * @param args the command line arguments
	 * @return job run exit code
	 */
	public int run(String[] args) throws Exception {
		if (null == getConf()) { // for YARN profile
			conf = new Configuration();
		}
		GiraphConfiguration giraphConf = new GiraphConfiguration(getConf());

		final int workers = Integer.parseInt(args[3]);
		final String inputPath = args[1];
		final String outputPath = args[2];

		giraphConf.setWorkerConfiguration(workers, workers, 100.0f);
		setWorkerContextClass(giraphConf);
		setMasterComputeClass(giraphConf);
		setVertexInputFormatClass(giraphConf);
		setVertexOutputFormatClass(giraphConf);
		setComputationClass(giraphConf);

		giraphConf.setZooKeeperConfiguration("master:2181");
		GiraphFileInputFormat.addVertexInputPath(giraphConf, new Path(inputPath));

		GiraphJob job = new GiraphJob(giraphConf, getJobName());
		prepareHadoopMRJob(job, outputPath);

		boolean verbose = true;
		return job.run(verbose) ? 0 : -1;
	}

	protected abstract String getJobName();

	protected void setVertexInputFormatClass(GiraphConfiguration gConf) {}

	protected void setVertexOutputFormatClass(GiraphConfiguration gConf) {}

	protected abstract void setComputationClass(GiraphConfiguration gConf);

	protected void setWorkerContextClass(GiraphConfiguration gConf) {}

	protected void setMasterComputeClass(GiraphConfiguration gConf) {}

	/**
	 * Populate internal Hadoop Job (and Giraph IO Formats) with Hadoop-specific
	 * configuration/setup metadata, propagating exceptions to calling code.
	 * 
	 * @param job
	 *          the GiraphJob object to help populate Giraph IO Format data.
	 * @param cmd
	 *          the CommandLine for parsing Hadoop MR-specific args.
	 */
	private void prepareHadoopMRJob(final GiraphJob job, final String outPath)
			throws Exception {
		FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(outPath));
	}

}
