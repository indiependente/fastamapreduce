package simple;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import simple.utils.HdfsLoader;

public class FastaMapper extends Mapper<LongWritable, Text, IntWritable, Text>  
{
	private static Log logger = LogFactory.getLog(FastaMapper.class);
	
	private String fastaPath; 
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		/*
		URI[] cachedFiles = context.getCacheFiles();
		
		for (URI u : cachedFiles)
			System.out.println("-> " + u.getPath());
		fastaPath = cachedFiles[0].getPath();
		
		*/
		
		fastaPath = "fasta36";
		
	}

	public String writeToFile(String name, String body)
	{
		String ret = "";
		PrintStream printer = null;
		try 
		{
			File tmp = File.createTempFile(name, ".tmp");
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
		StringBuilder builder = new StringBuilder("");
		ProcessBuilder runner = null;
		List<String> arguments = new ArrayList<String>();
		String[] w = {"", ""};
		String[] paths = new String[w.length];
		logger.info("starting...");
		try 
		{
			arguments.add(fastaPath);
			arguments.add("-q");
			
			String line = value.toString();
			if (line == null || line.length() <= 0)
				return;
			logger.info(line);
			w = line.split(HdfsLoader.DELIMITER);
			
			if (w.length != 2)
			{
				logger.info("problems here");
				return;
			}
			
			paths[0] = writeToFile("reference", w[0].replaceAll(HdfsLoader.CHAR_TO_REPLACE, "\n"));
			paths[1] = writeToFile("query", w[1].replaceAll(HdfsLoader.CHAR_TO_REPLACE, "\n"));
	
			for (String s : paths)
				arguments.add(s);
			
			runner = new ProcessBuilder(arguments);
			runner.redirectErrorStream(true);
			logger.info("running...");
			Process p = runner.start();
			
			InputStream stdin = p.getInputStream();
			InputStreamReader isr = new InputStreamReader(stdin);
			BufferedReader buffer = new BufferedReader(isr);
	
			line = null;
	
			while ((line = buffer.readLine()) != null) 
			{
				builder.append(line + "\n");
				logger.info(line);
			}
			
			p.waitFor(); // it retuns the exit value..
		}
		catch (Exception e)
		{
			logger.info(e.getMessage());
			e.printStackTrace();
		}
		
		Text result = new Text(builder.toString());
		context.write(new IntWritable(w[0].hashCode()), result);
		context.write(new IntWritable(w[1].hashCode()), result);
		
		for (String s : paths)
			(new File(s)).deleteOnExit();

	}
	
	
	
}
