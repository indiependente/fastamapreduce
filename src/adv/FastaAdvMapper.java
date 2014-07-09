package adv;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import simple.FastaReducer;
import simple.FastaSimpleJob;
import utils.BinRunner;
import utils.HdfsLoader;

public class FastaAdvMapper extends Mapper<LongWritable, Text, IntWritable, Text>  
{
	private static Log LOG = LogFactory.getLog(FastaReducer.class);
	
	private String path;
	private static final String WORKING_DIR = "/home/hduser/Scrivania";


	private String fastaPath;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		super.setup(context);
		Configuration cfg = context.getConfiguration();
		String fname = cfg.get(FastaAdvancedJob.WORKING_FILE_NAME);
		LOG.info("setup " + fname);
		FileSystem fs = FileSystem.get(cfg);
		path = "/home/hduser/Scrivania/" + FastaAdvancedJob.TARGET;
		fs.copyToLocalFile(new Path(FastaAdvancedJob.TARGET), new Path(path));
		fastaPath = "/home/hduser/Scrivania/fasta36";
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		super.cleanup(context);
		(new File(path)).delete();
	}
	
	public String writeToFile(String name, String body)
	{
		String ret = "";
		PrintStream printer = null;
		try 
		{
			File tmp = new File("/home/hduser/Scrivania/" + name + ".tmp");
			printer = new PrintStream(tmp);
			printer.print(body);
			ret = tmp.getAbsolutePath();
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			if (printer != null)
			{
				printer.close();
			}
		}
		return ret;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		List<String> arguments = new ArrayList<String>();
		String absPath = null;
		String line = "";
		File ref = new File(path);
		LOG.info("starting with " + fastaPath + "...");
		try 
		{
			arguments.add(fastaPath);
			arguments.add("-q");
			arguments.add(ref.getAbsolutePath());
			
			line = value.toString();
			if (line == null || line.length() <= 0)
			{
				LOG.info("invalid line");
				return;
			}
			
			
			String tmpFile = writeToFile("query", line.replaceAll(FastaSimpleJob.CHAR_TO_REPLACE, "\n"));
	
			arguments.add(tmpFile);
			
			absPath = BinRunner.execute(fastaPath, WORKING_DIR, arguments);
			
		
		}
		catch (Exception e)
		{
			LOG.info(e.getMessage());
			e.printStackTrace();
		}
		
		String toWrite = new String(Files.readAllBytes((new File(absPath)).toPath()));
		Text result = new Text(toWrite);
		
		context.write(new IntWritable(line.hashCode()), result);
		
		toWrite = null; // can I call System.gc() now?
		
		
	}

	

}
