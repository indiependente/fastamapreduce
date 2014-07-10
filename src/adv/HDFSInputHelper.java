package adv;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HDFSInputHelper {

	private static Log logger = LogFactory.getLog(HDFSInputHelper.class);

	public static String md5(String input)
	{
		return DigestUtils.md5Hex(input);
    }
	
	static Map<String, String> prepareInputFile(String inputDirPath, String outFileName, String delimitator) 
	{
		File folder = new File(inputDirPath);
		File[] listOfFiles = folder.listFiles();
		
		File fileInput = new File(outFileName);
		
		String fContent = null;
		String fName = null;
		
		Map<String, String> ret = new HashMap<String, String>();
		
		
		PrintWriter writer = null;
		
		try 
		{
			writer = new PrintWriter(fileInput);
		} 
		catch (FileNotFoundException e1) 
		{
			logger.fatal("Fatal error: Cannot create fileInput");
			System.exit(-1);
		}

		
		try{
			
			for (int i=0; i<listOfFiles.length; i++)
			{

				fContent = FileUtils.readFileToString(listOfFiles[i]).replaceAll("\r", "").replaceAll("\n", delimitator );
				fName = listOfFiles[i].getName();

				writer.print(fContent + "\n");

				ret.put(fName, md5(fContent));		
			}
			
		}catch (IOException e) {
			logger.fatal("Fatal error: Cannot read file " + fName+ " or cannot write its content.");
			System.exit(-1);

		}
		finally
		{
			writer.close();
		}

		if(ret.isEmpty())
		{	
			try {
				throw new Exception(inputDirPath+" is empty.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return ret;		
	}

}
