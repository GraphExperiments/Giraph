
package common;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import shortestpath.hbase.HBaseShortestPathRunner;
import shortestpath.hdfs.HdfsShortestPathRunner;
import utils.HiveRunner;

public class Runner {

	public static void main(String[] args) throws Exception {
		if (StringUtils.equalsIgnoreCase(args[0], "hdfs")) {
			System.exit(ToolRunner.run(new HdfsShortestPathRunner(), args));
		} else if (StringUtils.equalsIgnoreCase(args[0], "hive")) {
			System.exit(ToolRunner.run(new HiveRunner(), args));
		} else if (StringUtils.equalsIgnoreCase(args[0], "hbase")) {
			System.exit(ToolRunner.run(new HBaseShortestPathRunner(), args));
		}
	}
}
