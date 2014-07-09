package adv;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import simple.FastaReducer;
import utils.HdfsLoader;

public class FastaAdvMapper extends Mapper<LongWritable, Text, Text, Text>  
{
	private static Log LOG = LogFactory.getLog(FastaReducer.class);
	
	private String path;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		super.setup(context);
		Configuration cfg = context.getConfiguration();
		String fname = cfg.get(FastaAdvancedJob.WORKING_FILE_NAME);
		LOG.info("setup " + fname);
		HdfsLoader hdfs = HdfsLoader.getInstance().setup(cfg);
		path = "/home/hduser/Scrivania" + FastaAdvancedJob.TARGET;
		hdfs.copyFromHdfs(FastaAdvancedJob.TARGET, path);
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		super.cleanup(context);
		(new File(path)).delete();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		File f = new File(path);
		LOG.info("ance " + f.exists());
	}

	

}
