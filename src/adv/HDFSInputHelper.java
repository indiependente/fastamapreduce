package adv;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class HDFSInputHelper {

	private static Log logger = LogFactory.getLog(HDFSInputHelper.class);

	
	static ArrayList<String> prepare(String inputDirPath, String delimitator) 
	{
		File folder = new File(inputDirPath);
		File[] listOfFiles = folder.listFiles();
		
		File fileInput = new File("fileInput");
		
		String fContent = null;
		String fName = null;
		
		ArrayList<String> listOfFilesReturns = new ArrayList<String>();
		
		
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
			
			for(int i=0; i<listOfFiles.length; i++)
			{

				fContent = FileUtils.readFileToString(listOfFiles[i]).replaceAll("\r", "").replaceAll("\n", delimitator );
				fName = listOfFiles[i].getName();

				writer.print(fContent + "\n");

				listOfFilesReturns.add(fName);		
			}
			
		}catch (IOException e) {
			logger.fatal("Fatal error: Cannot read file " + fName+ " or cannot write its content.");
			System.exit(-1);

		}
		finally
		{
			writer.close();
		}

		if(listOfFilesReturns.isEmpty())
		{	
			try {
				throw new Exception(inputDirPath+" is empty.");
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return listOfFilesReturns;		
	}
		
}
