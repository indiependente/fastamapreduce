package simple.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import driver.ConfigurationLoader;

public class HdfsLoader 
{
	private static final String DELIMITER = "$$$";
	private static String OUTPUT_NAME = "BIGFILE";
	private static String CHAR_TO_REPLACE = "%";

	private Map<String, Integer> checksums;
	private Map<Integer, String> inverseChecksums;
	private Map<Integer, SimpleEntry<String,String>> coupleMap;
	private static Log logger = LogFactory.getLog(HdfsLoader.class);
	private int lineNo = 0;

	private FileSystem fs;
	private FSDataInputStream in;
	private FSDataOutputStream out;
	private Path toHdfs;

	public HdfsLoader(String inputDirPath)
	{
		toHdfs = new Path(OUTPUT_NAME);
		ConfigurationLoader loader = ConfigurationLoader.getInstance();
		try 
		{
			fs = FileSystem.get(loader.getConfiguration());
		} catch (IOException e) {
			logger.fatal("Fatal error: Cannot get the FileSystem from Hadoop configuration");
			System.exit(-1);
		}

		processInput(inputDirPath);
	}

	public void processInput(String inputDirPath){

		File folder = new File(inputDirPath);
		File[] listOfFiles = folder.listFiles();
		File output = new File(OUTPUT_NAME);
		PrintWriter writer = null;
		String f1Content = null;
		String f1NameFile = null;
		String f2Content = null;
		File f1 = null;
		File f2 = null;

		try 
		{
			writer = new PrintWriter(output);
		} 
		catch (FileNotFoundException e1) 
		{
			logger.fatal("Fatal error: Cannot create file " + OUTPUT_NAME);
			System.exit(-1);
		}

		for(int i=0; i<listOfFiles.length; i++)
		{
			try 
			{
				f1Content = FileUtils.readFileToString(listOfFiles[i]).replace("\n", CHAR_TO_REPLACE);;
				f1NameFile = listOfFiles[i].getName();
			} 
			catch (IOException e) {
				logger.fatal("Fatal error: Cannot read file " + listOfFiles[i]);
				System.exit(-1);

			}

			checksums.put(f1NameFile, getHash(f1Content));
			inverseChecksums.put(getHash(f1Content), f1NameFile);

			for(int j=i+1; j<listOfFiles.length; j++){
				try 
				{
					f2Content = FileUtils.readFileToString(listOfFiles[j]).replace("\n", CHAR_TO_REPLACE);;
					writer.print(f1Content + DELIMITER + f2Content + "\n");
					coupleMap.put(lineNo, new SimpleEntry<String, String>(f1.getName(), f2.getName()) );
					lineNo++;
				}
				catch (IOException e) {
					logger.fatal("Fatal error: Cannot read file " + listOfFiles[j]);
					System.exit(-1);
				}

			}
		}
		writer.close();
		writeOnHDFS(output);
		
	}


	public void loadOnHDFS(String path){

	}


	private int getHash(String s) {
		return s.hashCode();
	}


	public int getChecksum(String file){
		return checksums.get(file);
	}

	public String getFileNameFromHash(String hash){
		return inverseChecksums.get(hash);
	}

	/**
	 * Return the file's names of the couple at the row number @param row
	 * @param row the file line number 
	 * @return the array of string containing the file's names at the row @param row
	 */
	public String[] getCoupleAtRow(int row){
		return new String[]{
				coupleMap.get(row).getKey(),
				coupleMap.get(row).getValue()
		};
	}


	public void writeOnHDFS(File f) {
		
		FSDataOutputStream stream = null;
		BufferedWriter br = null;
		try
		{
			stream = fs.create(toHdfs,true);
			br = new BufferedWriter(new OutputStreamWriter(stream));
			String s = FileUtils.readFileToString(f);
			br.write(s);
			
		}
		catch(Exception e){
			System.out.println("File not found");
		}
		finally
		{
			try {
				if (br != null)
					br.close();
				
				if (stream != null)
					stream.close();
				
				fs.close();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	


}
