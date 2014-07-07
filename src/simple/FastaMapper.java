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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import simple.utils.HdfsLoader;

public class FastaMapper extends Mapper<LongWritable, Text, LongWritable, Text>  
{
	private static Log logger = LogFactory.getLog(FastaMapper.class);
	
	private String fastaPath; 
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		URI[] cachedFiles = context.getCacheFiles();
		
		fastaPath = cachedFiles[0].getPath();
		
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
		try 
		{
			arguments.add(fastaPath);
			arguments.add("-q");
			
			String line = value.toString();
			String[] w = line.split(HdfsLoader.DELIMITER);
			String[] paths = new String[w.length];
			
			paths[0] = writeToFile("reference", w[0]);
			paths[1] = writeToFile("query", w[1]);
	
			for (String s : paths)
				arguments.add(s);
			
			runner = new ProcessBuilder(arguments);
			runner.redirectErrorStream(true);
			
			Process p = runner.start();
			
			InputStream stdin = p.getInputStream();
			InputStreamReader isr = new InputStreamReader(stdin);
			BufferedReader buffer = new BufferedReader(isr);
	
			line = null;
	
			while ((line = buffer.readLine()) != null) 
			{
				builder.append(line + "\n");
			}
			
			p.waitFor(); // it retuns the exit value..
		}
		catch (Exception e)
		{
			logger.info(e.getMessage());
			e.printStackTrace();
		}
		
		context.write(new LongWritable(0), new Text(builder.toString()));
		context.write(new LongWritable(1), new Text(builder.toString()));

	}
	
	
	
}
