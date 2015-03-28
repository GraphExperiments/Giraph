
package hdfs;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

import common.PatternMatchingVertexValue;

public class PatternMatchingInputFormat extends
		TextVertexInputFormat<LongWritable, PatternMatchingVertexValue, Text> {

	@Override
	public TextVertexReader createVertexReader(InputSplit split,
			TaskAttemptContext context) throws IOException {
		return new LongVertexValueTextVertexReader();
	}

	class LongVertexValueTextVertexReader extends TextVertexReaderFromEachLine {

		@Override
		protected Iterable<Edge<LongWritable, Text>> getEdges(Text line)
				throws IOException {
			System.out.println("Start Read: " + String.valueOf(new Date().getTime()));
			String[] tokens = line.toString().split("\\t");
			List<Edge<LongWritable, Text>> edges = Lists
					.newArrayListWithCapacity(tokens.length - 1);
			for (int n = 1; n < tokens.length; n++) {
				LongWritable targetId = new LongWritable(Long.parseLong(tokens[n]
						.split(":")[0].trim()));
				Text targetLabel = new Text(tokens[n].split(":")[1].trim());
				edges.add(EdgeFactory.create(targetId, targetLabel));
			}
			System.out
					.println("Finish Read: " + String.valueOf(new Date().getTime()));
			return edges;
		}

		@Override
		protected LongWritable getId(Text line) throws IOException {
			return new LongWritable(Long.parseLong(line.toString().split("\\t")[0]
					.split(":")[0].trim()));
		}

		@Override
		protected PatternMatchingVertexValue getValue(Text line) throws IOException {
			return new PatternMatchingVertexValue(
					line.toString().split("\\t")[0].split(":")[1].trim());
		}

	}

}
