package simple;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FastaReducer extends Reducer<LongWritable, Text, Text, Text> 
{
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		StringBuilder res = new StringBuilder("");
		
		for (Text t : values)
		{
			res.append(t.toString());
			res.append("\n\n");
		}
		
		context.write(new Text("1"), new Text(res.toString()));
	}
}
