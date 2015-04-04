
package pagerank.hdfs;

import java.io.IOException;
import java.util.Date;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class PageRankVertexOutputFormat extends
		TextVertexOutputFormat<LongWritable, DoubleWritable, DoubleWritable> {

	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		return new PageRankVertexWriter();
	}

	public class PageRankVertexWriter extends TextVertexWriter {

		@Override
		public void writeVertex(
				Vertex<LongWritable, DoubleWritable, DoubleWritable> vertex)
				throws IOException, InterruptedException {
			System.out
					.println("Start Write: " + String.valueOf(new Date().getTime()));
			getRecordWriter().write(new Text(vertex.getId().toString()),
					new Text(vertex.getValue().toString()));
			System.out.println("Finish Write: "
					+ String.valueOf(new Date().getTime()));
		}
	}
}
