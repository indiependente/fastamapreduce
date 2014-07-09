package simple;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class FastaReducer extends Reducer<IntWritable, Text, Text, Text> 
{
	private static Log logger = LogFactory.getLog(FastaReducer.class);
	
	private MultipleOutputs<Text, Text> out;
	
	private FileSystem fs = null;
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		out.close();

	}



	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		out = new MultipleOutputs<Text, Text>(context);
		fs = FileSystem.get(context.getConfiguration());
	}


/*
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		StringBuilder res = new StringBuilder("");
		Configuration cfg = context.getConfiguration();
		String ref = cfg.get("" + key.get());
		for (Text t : values)
		{
			res.append(t.toString());
			res.append("\n\n");
		}
		out.write(new Text(ref), new Text(res.toString()), "text");
	}
*/
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		Configuration cfg = context.getConfiguration();
		String ref = cfg.get("" + key.get()); // file name
		// use create and write Text object there
		Path path = new Path(ref);
		FSDataOutputStream outStream = fs.create(path, true);
		for (Text t : values)
		{
			t.write(outStream);
		}
		outStream.close();
		// write on context <key, path to file on hdfs>"
		context.write(new Text(ref), new Text(path.toString()));
	}
	
	
}
