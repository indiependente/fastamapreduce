package adv;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import simple.FastaSimpleJob;
import utils.HdfsLoader;

public class FastaAdvancedJob extends Configured implements Tool 
{
	private static Log LOG = LogFactory.getLog(FastaAdvancedJob.class);

	public static final String TARGET = "TARGET";
	public static final String INPUT_NAME = "BIGFILE";
	public static final String OUTPUT_NAME = "OUTPUT_";
	public static final String DELIMITATOR = "%";
	public static String ALIGNMENTS_DIR = "ALIGNMENTS_";
	
	public static final String WORKING_FILE_NAME = "fastamapreduce.working.file";
	public static final String TARGET_CHECKSUM = "fastamapreduce.working.md5";
	
	public static final String MAPREDUCE_LINE_PER_MAPPER_PROPERTY = "mapreduce.input.lineinputformat.linespermap";
	public static final int MAPREDUCE_LINE_PER_MAPPER = 1;
	
	public static String cleanOutputName(String s)
	{
		return s.replaceAll(".", "").replaceAll("-", "").replaceAll("_", "");
	}
	
	
	
	@Override
	public int run(String[] args) throws Exception 
	{
		GenericOptionsParser parser = new GenericOptionsParser(getConf(), args);
		String[] argv = parser.getRemainingArgs();
		LOG.info("Starting....");
		HdfsLoader loader = HdfsLoader.getInstance().setup(getConf());
		String inputDir = argv[0];
		LOG.info("preparing input....");
		Map<String, String> checksums = HDFSInputHelper.prepareInputFile(inputDir, INPUT_NAME, DELIMITATOR);
		loader.copyOnHdfs(INPUT_NAME, INPUT_NAME);
		LOG.info("input ready....");
		
		List<String> keys = new ArrayList<String>(checksums.keySet());
		long totalTime = System.currentTimeMillis();
		int numOfFiles = keys.size();
		LOG.info("launching " + numOfFiles + " jobs....");
		for (int i = 0; i < numOfFiles; i++)
		{
			LOG.info("launching job " + i + " of " + numOfFiles + " ....");
			String file = keys.get(i);
			Configuration config = getConf();
			config.setInt(FastaSimpleJob.MAPREDUCE_LINERECORD_LENGTH, Integer.MAX_VALUE);
			Job job = Job.getInstance(config, getClass().getSimpleName());
			config = job.getConfiguration(); // it must be done since it changes after job init
			
			config.set(WORKING_FILE_NAME, file);
			config.set(TARGET_CHECKSUM, checksums.get(file));
			
			String outputDir = OUTPUT_NAME + i;
			String alignDir = ALIGNMENTS_DIR + file;
			
			loader.deleteFromHdfs(TARGET);
			loader.deleteFromHdfs(outputDir);
			loader.deleteFromHdfs(alignDir);
			loader.mkdir(alignDir);
			loader.copyOnHdfs(inputDir + "/" + file, TARGET);
			
//			job.addCacheFile(new Path(TARGET).toUri());
			
			job.setJarByClass(FastaAdvancedJob.class);
			job.setMapperClass(FastaAdvMapper.class);
//			job.setCombinerClass(FastaAdvReducer.class);
			job.setReducerClass(FastaAdvReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			
			MultipleOutputs.addNamedOutput(job, checksums.get(file), TextOutputFormat.class, Text.class, Text.class);
			
			
			job.setInputFormatClass(NLineInputFormat.class);
			
			NLineInputFormat.addInputPath(job, new Path(INPUT_NAME));
			
		    FileOutputFormat.setOutputPath(job, new Path(outputDir));
		    
		    config.setInt(MAPREDUCE_LINE_PER_MAPPER_PROPERTY, MAPREDUCE_LINE_PER_MAPPER);
		    
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
		
		loader.deleteFromHdfs(TARGET);
    	LOG.info("All jobs[" + numOfFiles + "] completed in " + (System.currentTimeMillis() - totalTime) + " ms");

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
