package simple;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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

import simple.utils.HdfsLoader;

public class FastaSimpleJob extends Configured implements Tool 
{
	private static Log logger = LogFactory.getLog(FastaSimpleJob.class);
	public static final String FASTA_BIN_PATH = "./fasta36";
	
	public static final String MAPREDUCE_LINERECORD_LENGTH = "mapreduce.input.linerecordreader.line.maxlength";
	
	public static final String DELIMITER = "@@@";
	public static String INPUT_NAME = "BIGFILE";
	public static String CHAR_TO_REPLACE = "%";
	
	private String inputDir;
	private Map<String, Integer> checksums;
	
	
	public void prepareInputForHdfs()
	{
		File folder = new File(inputDir);
		File[] listOfFiles = folder.listFiles();
		File output = new File(INPUT_NAME);	
		PrintStream writer = null;
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
	
				checksums.put(fileNameX, fileContentX.hashCode());
	
				for (int j = i + 1; j < listOfFiles.length; j++)
				{
					try 
					{
						fileNameY = listOfFiles[j].getName();
						fileContentY = FileUtils.readFileToString(listOfFiles[j]).replaceAll("\n", CHAR_TO_REPLACE).replaceAll("\r", "").trim();
						writer.print(fileContentX + DELIMITER + fileContentY + "\n");
						checksums.put(fileNameY, fileContentY.hashCode());
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
	}
	
	
	
	@Override
	public int run(String[] args) throws Exception 
	{
		Configuration config = getConf(); //ConfigurationLoader.getInstance().getConfiguration();
		config.setInt(MAPREDUCE_LINERECORD_LENGTH, Integer.MAX_VALUE);
		GenericOptionsParser parser = new GenericOptionsParser(config, args);
		String[] argv = parser.getRemainingArgs();
		
		checksums = new HashMap<String, Integer>();
		inputDir = argv[0];
		
		Job job = Job.getInstance(config, getClass().getSimpleName());
//		HdfsLoader loader = new HdfsLoader(config, argv[0]);
		
		HdfsLoader loader = HdfsLoader.getInstance();
		loader.setup(job.getConfiguration());
		
		prepareInputForHdfs();
		
		loader.copyOnHdfs(INPUT_NAME, INPUT_NAME);
		loader.deleteFromHdfs("OUTPUT");
		
		job.setJarByClass(FastaSimpleJob.class);
		job.setMapperClass(FastaMapper.class);
		job.setCombinerClass(FastaReducer.class);
		job.setReducerClass(FastaReducer.class);
		
		
		// load fasta36 in distributed cache
		
		//job.addCacheFile(new Path("fasta36").toUri());
		//job.createSymlink();
		/*	
		loader.deleteFromHdfs(FASTA_BIN_PATH);
		loader.copyOnHdfs(FASTA_BIN_PATH, FASTA_BIN_PATH);
		
		DistributedCache.addCacheFile(new Path(FASTA_BIN_PATH).toUri(), job.getConfiguration());
		DistributedCache.createSymlink(job.getConfiguration());
	*/	
		for (Entry<String, Integer> e : checksums.entrySet())
		{
			job.getConfiguration().setInt(e.getKey(), e.getValue());
			job.getConfiguration().set("" + e.getValue(), e.getKey());
		}
			
		// map <long, text> --> <long, text> 
		// reduce <int, list(text)> --> <text, text>
				
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, Text.class, Text.class);

		
	    FileInputFormat.addInputPath(job, new Path(INPUT_NAME));
	    FileOutputFormat.setOutputPath(job, new Path("OUTPUT"));
	    
	    long startTime = System.currentTimeMillis();
	    int result = job.waitForCompletion(true) ? 0 : 1;
		
	    logger.info("Job completed in " + (System.currentTimeMillis() - startTime) + " ms");

	    (new File(INPUT_NAME)).deleteOnExit();
	    
	    return result;
	}


	public static void main(String[] args) {
		int result = -1;
		try 
		{
			result = ToolRunner.run(new FastaSimpleJob(), args);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			System.out.println("Job failed.");
		}
		System.exit(result);
	}

}
