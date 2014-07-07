package simple;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FastaReducer extends Reducer<IntWritable, Text, Text, Text> 
{
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		StringBuilder res = new StringBuilder("");
		
		for (Text t : values)
		{
			res.append(t.toString());
			res.append("\n\n");
		}
		
		context.write(new Text(context.getConfiguration().get("" + key.get())), new Text(res.toString()));
	}
}
