package simple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;

import simple.utils.HdfsLoader;

public class Fasta extends Configured implements Tool 
{
	private static Log logger = LogFactory.getLog(Fasta.class);

	@Override
	public int run(String[] args) throws Exception 
	{
		Job job = Job.getInstance(getConf(), getClass().getSimpleName());
		HdfsLoader loader = new HdfsLoader("");

		job.setJarByClass(Fasta.class);
		job.setMapperClass(FastaMapper.class);
		job.setCombinerClass(FastaReducer.class);
		job.setReducerClass(FastaReducer.class);
		
		// load fasta36 in distributed cache
		
		job.addCacheFile(new Path("./fasta/fasta36").toUri());
		
		// map <long, text> --> <long, text> 
		// reduce <long, list(text)> --> <text, text>
				
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(""));
	    FileOutputFormat.setOutputPath(job, new Path(""));
	    
	    long startTime = System.currentTimeMillis();
	    int result = job.waitForCompletion(true) ? 0 : 1;
		
	    logger.info("Job completed in " + (System.currentTimeMillis() - startTime) + " ms");
	    
		return result;
	}


	public static void main(String[] args) {
		int result = 1;
		try 
		{
			result = ToolRunner.run(new Fasta(), args);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			System.out.println("Job failed.");
		}
		System.exit(result);
	}

}
