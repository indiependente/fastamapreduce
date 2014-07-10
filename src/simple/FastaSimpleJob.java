package simple;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.ParseException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import driver.ConfigurationLoader;

import utils.HdfsLoader;

public class FastaSimpleJob extends Configured implements Tool 
{
	private static Log logger = LogFactory.getLog(FastaSimpleJob.class);
	public static final String FASTA_BIN_PATH = "./fasta36";
	
	public static final String MAPREDUCE_LINERECORD_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";
	
	public static final String DELIMITER = "@@@";
	public static String INPUT_NAME = "BIGFILE";
	public static String ALIGNMENTS_DIR = "ALIGNMENTS";
	public static String CHAR_TO_REPLACE = "%";
	
	private String inputDir;
	private Map<String, String> checksums;
	
	
	public int prepareInputForHdfs()
	{
		File folder = new File(inputDir);
		File[] listOfFiles = folder.listFiles();
		File output = new File(INPUT_NAME);	
		PrintStream writer = null;
		logger.info("preparing input for hdfs...");
		try
		{
			writer = new PrintStream(output);
			
			for (int i = 0; i < listOfFiles.length - 1; i++)
			{
				String fileContentX = "", fileContentY = ""; 
				String fileNameX = "", fileNameY = ""; 
				try 
				{
					fileContentX = FileUtils.readFileToString(listOfFiles[i]).replaceAll("\r", "").replaceAll("\n", CHAR_TO_REPLACE).trim();
					fileNameX = listOfFiles[i].getName();
				} 
				catch (IOException e)
				{
					logger.fatal("Fatal error: Cannot read file " + listOfFiles[i]);
					System.exit(-1);
	
				}
	
				checksums.put(fileNameX, md5(fileContentX));
	
				for (int j = i + 1; j < listOfFiles.length; j++)
				{
					try 
					{
						fileNameY = listOfFiles[j].getName();
						fileContentY = FileUtils.readFileToString(listOfFiles[j]).replaceAll("\n", CHAR_TO_REPLACE).replaceAll("\r", "").trim();
						writer.print(fileContentX + DELIMITER + fileContentY + "\n");
						checksums.put(fileNameY, md5(fileContentY));
					}
					catch (IOException e) {
						logger.fatal("Fatal error: Cannot read file " + listOfFiles[j]);
						System.exit(-1);
					}
	
				}
			}
			
		}
		catch (IOException ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			if (writer != null)
				writer.close();
		}
		logger.info("input for hdfs ready...");
		return listOfFiles.length;
	}
	
	public static String md5(String input)
	{
		return DigestUtils.md5Hex(input);
    }
	
	
	
	@Override
	public int run(String[] args) throws Exception 
	{
		Configuration config = getConf(); //ConfigurationLoader.getInstance().getConfiguration();
		config.setInt(MAPREDUCE_LINERECORD_LENGTH, Integer.MAX_VALUE);
		GenericOptionsParser parser = new GenericOptionsParser(config, args);
		String[] argv = parser.getRemainingArgs();
		
		checksums = new HashMap<String, String>();
		inputDir = argv[0];
		
		
		logger.info("configuring job....");
		Job job = Job.getInstance(config, getClass().getSimpleName());
//		HdfsLoader loader = new HdfsLoader(config, argv[0]);
		
		
		HdfsLoader loader = HdfsLoader.getInstance();
		loader.setup(job.getConfiguration());
		
		int filesCounter = prepareInputForHdfs();
		
		JobClient jclient = new JobClient(job.getConfiguration());
		
		int numOfReducer = Math.min(filesCounter, jclient.getClusterStatus(true).getActiveTrackerNames().size());
		
		logger.info("selected # of reducers: " + numOfReducer);
		
		loader.copyOnHdfs(INPUT_NAME, INPUT_NAME);
		loader.deleteFromHdfs("OUTPUT");
		loader.deleteFromHdfs(ALIGNMENTS_DIR);
		loader.mkdir(ALIGNMENTS_DIR);
		
		job.setJarByClass(FastaSimpleJob.class);
		job.setMapperClass(FastaMapper.class);
		job.setCombinerClass(FastaReducer.class);
		job.setReducerClass(FastaReducer.class);
		
		job.setNumReduceTasks(numOfReducer);
		
		// load fasta36 in distributed cache
		
		//job.addCacheFile(new Path("fasta36").toUri());
		//job.createSymlink();
		/*	
		loader.deleteFromHdfs(FASTA_BIN_PATH);
		loader.copyOnHdfs(FASTA_BIN_PATH, FASTA_BIN_PATH);
		
		DistributedCache.addCacheFile(new Path(FASTA_BIN_PATH).toUri(), job.getConfiguration());
		DistributedCache.createSymlink(job.getConfiguration());
	*/	
		
			
		// map <long, text> --> <long, text> 
		// reduce <int, list(text)> --> <text, text>
				
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		config = job.getConfiguration();
		for (Entry<String, String> e : checksums.entrySet())
		{
			logger.info(e.getKey() + " => " + e.getValue());
			config.set(e.getKey(), e.getValue());
			config.set(e.getValue(), e.getKey());
			MultipleOutputs.addNamedOutput(job, e.getValue(), TextOutputFormat.class, Text.class, Text.class);
		}

		
	    FileInputFormat.addInputPath(job, new Path(INPUT_NAME));
	    FileOutputFormat.setOutputPath(job, new Path("OUTPUT"));
	    
	    long startTime = System.currentTimeMillis();
	    logger.info("starting job...");
	    int result = job.waitForCompletion(true) ? 0 : 1;
		
	    logger.info("Job completed in " + (System.currentTimeMillis() - startTime) + " ms");

	    Files.deleteIfExists(Paths.get(INPUT_NAME));
	    return result;
	}


	public static void main(String[] args)
	{
		int result = -1;
		try 
		{
			logger.info("Starting tool...");
			result = ToolRunner.run(new FastaSimpleJob(), args);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			logger.info("Job failed.");
		}
		System.exit(result);
	}

}
