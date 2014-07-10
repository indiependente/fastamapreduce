package simple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import utils.BinRunner;



public class FastaMapper extends Mapper<LongWritable, Text, Text, Text>  
{
	private static final String WORKING_DIR = "/home/hduser/Scrivania";

	private static Log logger = LogFactory.getLog(FastaMapper.class);
	
	private String fastaPath = ""; 
	
	private ArrayList<String> filesToDelete;
		
	@Override
	protected void setup(Context context) throws IOException, InterruptedException 
	{
		super.setup(context);
		
		Configuration config = context.getConfiguration();
//		FileSystem dfs = FileSystem.get(config);
	    
//		dfs.copyToLocalFile(new Path(FastaSimpleJob.FASTA_BIN_PATH), new Path(FastaSimpleJob.FASTA_BIN_PATH));
//		fastaPath = new Path(FastaSimpleJob.FASTA_BIN_PATH).toString();
		fastaPath = "/home/hduser/Scrivania/fasta36";
		filesToDelete = new ArrayList<String>();
	}
	
	
	

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
		for (String path : filesToDelete)
			java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(path));

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
		String[] w = {"", ""};
		String[] paths = new String[w.length];
		String absPath = null;
		
		logger.info("starting mapper with " + fastaPath + "...");
		try 
		{
			arguments.add(fastaPath);
			arguments.add("-q");
			
			String line = value.toString();
			if (line == null || line.length() <= 0)
				return;
//			logger.info(line);
			w = line.split(FastaSimpleJob.DELIMITER);
			
			if (w.length != 2)
			{
				logger.info("problems here");
				return;
			}
			
			paths[0] = writeToFile("reference", w[0].replaceAll(FastaSimpleJob.CHAR_TO_REPLACE, "\n"));
			paths[1] = writeToFile("query", w[1].replaceAll(FastaSimpleJob.CHAR_TO_REPLACE, "\n"));
	
			for (String s : paths)
				arguments.add(s);
			
			final Context finalContext = context;
			absPath = BinRunner.execute(fastaPath, WORKING_DIR, arguments,
					new Runnable() {

						@Override
						public void run() {
							finalContext.progress();
						}
						
					}
			);
			filesToDelete.add(absPath);
		}
		catch (Exception e)
		{
			logger.info(e.getMessage());
			e.printStackTrace();
		}
		
		File absFile = new File(absPath);
		if (!absFile.exists())
			throw new FileNotFoundException("File not found: "+absFile.toString());
		
		String toWrite = new String(Files.readAllBytes(absFile.toPath()));
		Text result = new Text(toWrite);
		
		context.write(new Text(FastaSimpleJob.md5(w[0])), result);
		context.write(new Text(FastaSimpleJob.md5(w[1])), result);
		
		toWrite = null; // can I call System.gc() now?
		
		for (String s : paths)
			Files.delete(new File(s).toPath());

	}
	
	
	
	
}
