package adv;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import simple.FastaMapper;
import simple.FastaReducer;
import simple.FastaSimpleJob;
import utils.HdfsLoader;

public class FastaAdvancedJob extends Configured implements Tool 
{
	private static Log LOG = LogFactory.getLog(FastaAdvancedJob.class);

	public static final String TARGET = "TARGET";
	public static final String INPUT_NAME = "BIGFILE";
	public static final String DELIMITATOR = "%";
	
	public static final String WORKING_FILE_NAME = "fastamr.working.file";

	
	public static String cleanOutputName(String s)
	{
		return s.replaceAll(".", "").replaceAll("-", "").replaceAll("_", "");
	}
	
	
	
	@Override
	public int run(String[] args) throws Exception 
	{
		GenericOptionsParser parser = new GenericOptionsParser(getConf(), args);
		String[] argv = parser.getRemainingArgs();
		
		HdfsLoader loader = HdfsLoader.getInstance().setup(getConf());
		String inputDir = argv[0];
		
		Map<String, String> checksums = HDFSInputHelper.prepareInputFile(inputDir, INPUT_NAME, DELIMITATOR);
		loader.copyOnHdfs(INPUT_NAME, INPUT_NAME);
		loader.deleteFromHdfs("OUTPUT");
				
		List<String> keys = new ArrayList<String>(checksums.keySet());
		for (int i = 0, l = keys.size(); i < l; i++)
		{
			String file = keys.get(i);
			Configuration config = getConf();
			config.setInt(FastaSimpleJob.MAPREDUCE_LINERECORD_LENGTH, Integer.MAX_VALUE);
			Job job = Job.getInstance(config, getClass().getSimpleName());
			config = job.getConfiguration(); // it must be done since it changes after job init
			
			config.set(WORKING_FILE_NAME, file);
			
			loader.deleteFromHdfs(TARGET);
			loader.deleteFromHdfs("OUTPUT" + i);
			loader.copyOnHdfs(inputDir + "/" + file, TARGET);
			
			job.setJarByClass(FastaAdvancedJob.class);
			job.setMapperClass(FastaAdvMapper.class);
			job.setCombinerClass(FastaAdvReducer.class);
			job.setReducerClass(FastaAdvReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			
			MultipleOutputs.addNamedOutput(job, checksums.get(file), TextOutputFormat.class, Text.class, Text.class);
			
		    FileInputFormat.addInputPath(job, new Path(INPUT_NAME));
		    FileOutputFormat.setOutputPath(job, new Path("OUTPUT" + i));
		    
		    long startTime = System.currentTimeMillis();
		    if (job.waitForCompletion(true))
		    {
		    	LOG.info("Job completed for file [" + file + "] in " + (System.currentTimeMillis() - startTime) + " ms");
		    }
		    else
		    {
		    	LOG.info("Job failed for file [" + file + "] in " + (System.currentTimeMillis() - startTime) + " ms");
		    }
		}
		
		return 0;
	}

	public static void main(String[] args) 
	{
		int result = -1;
		try 
		{
			result = ToolRunner.run(new FastaAdvancedJob(), args);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			System.out.println("Job failed.");
		}
		System.exit(result);
	}
	
}
