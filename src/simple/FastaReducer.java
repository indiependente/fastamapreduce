package simple;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class FastaReducer extends Reducer<IntWritable, Text, Text, Text> 
{
	private static Log logger = LogFactory.getLog(FastaReducer.class);
	
	private MultipleOutputs<Text, Text> out;
	
	
	
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
	}



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
}
