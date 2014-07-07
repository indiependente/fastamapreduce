package simple;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FastaMapper extends Mapper<LongWritable, Text, LongWritable, Text>  
{
	private static Log logger = LogFactory.getLog(FastaMapper.class);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		
	}
	
	
	
}
