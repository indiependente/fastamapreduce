package adv;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import simple.FastaReducer;

public class FastaAdvReducer extends Reducer<Text, Text, Text, Text> {

	private static Log LOG = LogFactory.getLog(FastaReducer.class);
	
	private static final String RESULTS_NAME = "/results";
	
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



	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		Configuration cfg = context.getConfiguration();
		String refName = cfg.get(FastaAdvancedJob.WORKING_FILE_NAME);
		String alignmentDir = FastaAdvancedJob.ALIGNMENTS_DIR + refName;
		Path path = new Path(alignmentDir + RESULTS_NAME);
		
		{
			FSDataOutputStream outStream = (!fs.exists(path)) ? fs.create(path, true) : fs.append(path);
			
			Iterator<Text> it = values.iterator();
			while (it.hasNext())
			{
				it.next().write(outStream);
			}
			
			outStream.close();
		}
		
		out.write(new Text(refName), new Text(path.toString()), cfg.get(FastaAdvancedJob.TARGET_CHECKSUM));
	}
	
	
	
}
