package driver;

import org.apache.hadoop.util.ProgramDriver;

public class Driver {

	public static void main(String[] args)
	{	
		int returnCode = 0;
		try 
		{
			ProgramDriver driver = new ProgramDriver();
			
			returnCode = driver.run(args);
		}
		catch (Throwable e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
			returnCode = -1;
		}
		System.exit(returnCode);
	}
	
}
