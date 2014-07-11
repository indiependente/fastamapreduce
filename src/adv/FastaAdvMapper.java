package adv;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import simple.FastaReducer;
import simple.FastaSimpleJob;
import utils.BinRunner;

public class FastaAdvMapper extends Mapper<LongWritable, Text, Text, Text>  
{
	private static Log LOG = LogFactory.getLog(FastaReducer.class);
	
	private String path;
	private static final String WORKING_DIR = "/home/hduser/Scrivania";


	private String fastaPath;
	private String targetMd5;
	
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
		//path = "/home/hduser/Scrivania/" + FastaAdvancedJob.TARGET;
		
		URI[] cachedFiles = context.getCacheFiles();
		path = new Path(cachedFiles[0]).toString();
		//fs.copyToLocalFile(new Path(FastaAdvancedJob.TARGET), new Path(path));
		
		fastaPath = "/home/hduser/Scrivania/fasta36";
		String refContent = FileUtils.readFileToString(new File(path)).replaceAll("\r", "");
		targetMd5 = HDFSInputHelper.md5(refContent);
		
		
	}
/*
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException
	{
		super.cleanup(context);
		Files.delete(Paths.get(path));
	}
*/	
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
	
	public boolean checkForIdentity(String queryContent)
	{
		String md5Qry = HDFSInputHelper.md5(queryContent);
		return targetMd5.equals(md5Qry);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		List<String> arguments = new ArrayList<String>();
		String absPath = null;
		String line = "";
		String md5 = "";
		String tmpFile = "";
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
			
			
			line = line.replaceAll(FastaSimpleJob.CHAR_TO_REPLACE, "\n");
			
			if (checkForIdentity(line))
			{
				LOG.info("skipping identity " + context.getConfiguration().get(FastaAdvancedJob.WORKING_FILE_NAME));
				return;
			}
			
			tmpFile = writeToFile("query", line);
	
			arguments.add(tmpFile);
			
			final Context finalContext = context;
			
			absPath = BinRunner.execute(fastaPath, WORKING_DIR, arguments, 
				new Runnable() {
					private int lineCounter = 0;
					private final static int LINE_UPDATE = 1000;
					@Override
					public void run() {
						if (lineCounter % LINE_UPDATE == 0)
							finalContext.progress();
						lineCounter++;
					}
					
				
			});
			
		
		}
		catch (Exception e)
		{
			LOG.info(e.getMessage());
			e.printStackTrace();
		}
		
		String toWrite = new String(Files.readAllBytes((new File(absPath)).toPath()));
		Text result = new Text(toWrite);
		
		context.write(new Text(targetMd5), result);
		
		toWrite = null; // can I call System.gc() now?
		
		Files.delete(new File(absPath).toPath());
		Files.delete(new File(tmpFile).toPath());

	}

	

}
