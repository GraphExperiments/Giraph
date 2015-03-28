
package common;

import hdfs.PatternMatchingHdfsRunner;
import hive.PatternMatchingHiveRunner;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import patternmatching.hbase.PatterMatchingHBaseRunner;

public class Runner {

	public static void main(String[] args) throws Exception {
		if (StringUtils.equalsIgnoreCase(args[0], "hdfs")) {
			System.exit(ToolRunner.run(new PatternMatchingHdfsRunner(), args));
		} else if (StringUtils.equalsIgnoreCase(args[0], "hive")) {
			System.exit(ToolRunner.run(new PatternMatchingHiveRunner(), args));
		} else if (StringUtils.equalsIgnoreCase(args[0], "hbase")) {
			System.exit(ToolRunner.run(new PatterMatchingHBaseRunner(), args));
		}
	}
}
