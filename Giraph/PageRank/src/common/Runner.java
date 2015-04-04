
package common;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import pagerank.hbase.PageRankHBaseRunner;
import pagerank.hdfs.PageRankHdfsRunner;
import pagerank.hive.PageRankHiveRunner;

public class Runner {

	public static void main(String[] args) throws Exception {
		if (StringUtils.equalsIgnoreCase(args[0], "hdfs")) {
			System.exit(ToolRunner.run(new PageRankHdfsRunner(), args));
		} else if (StringUtils.equalsIgnoreCase(args[0], "hive")) {
			PageRankHiveRunner runner = new PageRankHiveRunner();
			System.exit(ToolRunner.run(runner, args));
		} else if (StringUtils.equalsIgnoreCase(args[0], "hbase")) {
			PageRankHBaseRunner runner = new PageRankHBaseRunner();
			System.exit(ToolRunner.run(runner, args));
		}
	}
}
