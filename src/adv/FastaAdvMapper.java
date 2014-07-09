package adv;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import simple.FastaReducer;

public class FastaAdvMapper extends Mapper<LongWritable, Text, Text, Text>  
{
	private static Log LOG = LogFactory.getLog(FastaReducer.class);
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		super.setup(context);
		String fname = context.getConfiguration().get(FastaAdvancedJob.WORKING_FILE_NAME);
		LOG.info(fname);
		File f = new File(fname);
		
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		
	}

	

}
