package simple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Fasta extends Configured implements Tool 
{

	@Override
	public int run(String[] arg0) throws Exception 
	{
		Configuration conf = new Configuration();
	   
		// Job is deprecated
		// https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapred/JobConf.html
		
		Job job = new Job(conf, "simple fasta");
		
		job.setJarByClass(Fasta.class);
		job.setMapperClass(FastaMapper.class);
		job.setCombinerClass(FastaReducer.class);
		job.setReducerClass(FastaReducer.class);
		
		// map <long, text> --> <long, text> 
		// reduce <long, list(text)> --> <text, text>
				
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
		
		return 0;
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
