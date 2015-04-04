package pagerank.hdfs;

import java.io.IOException;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class testt extends TextEdgeInputFormat<LongWritable, DoubleWritable> {

	@Override
	public EdgeReader<LongWritable, DoubleWritable> createEdgeReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
