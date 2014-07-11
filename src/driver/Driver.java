package driver;

import org.apache.hadoop.util.ProgramDriver;

import adv.FastaAdvancedJob;

import simple.FastaSimpleJob;

public class Driver {

	public static void main(String[] args)
	{	
		int returnCode = 0;
		try 
		{
			ProgramDriver driver = new ProgramDriver();
			
			driver.addClass("-s", FastaSimpleJob.class, "shit");
			driver.addClass("-a", FastaAdvancedJob.class, "moar shit");

			returnCode = driver.run(args);
		}
		catch (Throwable e) 
		{
			e.printStackTrace();
			returnCode = -1;
		}
		System.exit(returnCode);
	}
	
}
