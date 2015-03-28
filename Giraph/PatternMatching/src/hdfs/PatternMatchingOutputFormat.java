
package hdfs;

import java.io.IOException;
import java.util.Date;
import java.util.Map.Entry;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import common.PatternMatchingVertexValue;

public class PatternMatchingOutputFormat extends
		TextVertexOutputFormat<LongWritable, PatternMatchingVertexValue, Text> {

	private class LongVertexValueTextTextVertexWriter extends TextVertexWriter {

		@Override
		public void writeVertex(
				Vertex<LongWritable, PatternMatchingVertexValue, Text> vertex)
				throws IOException, InterruptedException {
			System.out
					.println("Start Write: " + String.valueOf(new Date().getTime()));
			StringBuilder sb = new StringBuilder();
			if (!vertex.getValue().getMatchSet().isEmpty()
					&& vertex.getValue().isMatch()) {
				for (Entry<Writable, Writable> idLable : vertex.getValue()
						.getMatchSet().entrySet()) {
					sb.append(idLable.getValue());
					sb.append("->");
				}
				getRecordWriter().write(new Text(String.valueOf(vertex.getId().get())),
						new Text(sb.toString()));
			}
			System.out.println("Finish Write: "
					+ String.valueOf(new Date().getTime()));
		}
	}

	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new LongVertexValueTextTextVertexWriter();
	}

}
