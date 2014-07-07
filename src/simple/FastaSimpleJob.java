package simple;

import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import simple.utils.HdfsLoader;

public class FastaSimpleJob extends Configured implements Tool 
{
	private static Log logger = LogFactory.getLog(FastaSimpleJob.class);

	@Override
	public int run(String[] args) throws Exception 
	{
		Job job = Job.getInstance(getConf(), getClass().getSimpleName());
		HdfsLoader loader = new HdfsLoader(args[0]);

		job.setJarByClass(FastaSimpleJob.class);
		job.setMapperClass(FastaMapper.class);
		job.setCombinerClass(FastaReducer.class);
		job.setReducerClass(FastaReducer.class);
		
		// load fasta36 in distributed cache
		
		job.addCacheFile(new Path("./fasta/fasta36").toUri());
		
		for (Entry<String, Integer> e : loader.getChecksums().entrySet())
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
	    
	    FileInputFormat.addInputPath(job, new Path(HdfsLoader.OUTPUT_NAME));
	    FileOutputFormat.setOutputPath(job, new Path("OUTPUT"));
	    
	    long startTime = System.currentTimeMillis();
	    int result = job.waitForCompletion(true) ? 0 : 1;
		
	    logger.info("Job completed in " + (System.currentTimeMillis() - startTime) + " ms");
	    
		return result;
	}


	public static void main(String[] args) {
		int result = 1;
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
