package utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

public class Utils 
{

	public static String writeToFile(String name, String body)
	{
		String ret = "";
		PrintStream printer = null;
		try 
		{
//			File tmp = new File("/home/hduser/Scrivania/" + name + ".tmp");
			File tmp = new File(String.format(Defines.TEMP_NAME, name));
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
	
}
