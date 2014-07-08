package simple.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.FileSystemNotFoundException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsLoader 
{
	public static final String DELIMITER = "$$$";
	public static String INPUT_NAME = "BIGFILE";
	public static String CHAR_TO_REPLACE = "%";

	private static HdfsLoader instance;
	private Map<String, Integer> checksums;
//	private Map<Integer, String> inverseChecksums;
	private Map<Integer, SimpleEntry<String,String>> coupleMap;
	private static Log logger = LogFactory.getLog(HdfsLoader.class);
	private int lineNo = 0;

	private FileSystem fs;
//	private FSDataInputStream in;
//	private FSDataOutputStream out;
	private Path toHdfs;
	private Configuration configuration;
	
	private HdfsLoader(){
		checksums = new HashMap<String, Integer>();
		coupleMap = new HashMap<Integer, SimpleEntry<String,String>>();
	}
	
	public static HdfsLoader getInstance(){
		if(instance == null)
			instance = new HdfsLoader();
		return instance;
	}
	
	public void setup(Configuration config, String inputDirPath){
		toHdfs = new Path(INPUT_NAME);
		
		this.configuration = config;
		
		logger.info("> Input File Name: " + INPUT_NAME);
		
		openFS();
		
		processInput(inputDirPath);
	}
	
	private void openFS(){
		try 
		{
			fs = FileSystem.get(configuration);
			logger.info("> Home directory: " + fs.getHomeDirectory());
		} catch (IOException e) {
			logger.fatal("Fatal error: Cannot get the FileSystem from Hadoop configuration");
			System.exit(-1);
		}
	}
	
	
	public void processInput(String inputDirPath){

		File folder = new File(inputDirPath);
		File[] listOfFiles = folder.listFiles();
		File output = new File(INPUT_NAME);
		PrintWriter writer = null;
		String f1Content = null;
		String f1NameFile = null;
		String f2Content = null;
//		File f1 = null;
//		File f2 = null;

		try 
		{
			writer = new PrintWriter(output);
		} 
		catch (FileNotFoundException e1) 
		{
			logger.fatal("Fatal error: Cannot create file " + INPUT_NAME);
			System.exit(-1);
		}

		for(int i=0; i<listOfFiles.length-1; i++)
		{
			try 
			{
				f1Content = FileUtils.readFileToString(listOfFiles[i]).replaceAll("\r", "").replaceAll("\n", CHAR_TO_REPLACE);
				f1NameFile = listOfFiles[i].getName();
			} 
			catch (IOException e) {
				logger.fatal("Fatal error: Cannot read file " + listOfFiles[i]);
				System.exit(-1);

			}

			checksums.put(f1NameFile, getHash(f1Content));
//			inverseChecksums.put(getHash(f1Content), f1NameFile);

			for(int j=i+1; j<listOfFiles.length; j++){
				try 
				{
					f2Content = FileUtils.readFileToString(listOfFiles[j]).replaceAll("\n", CHAR_TO_REPLACE).replaceAll("\r", "");
					writer.print(f1Content + DELIMITER + f2Content + "\n");
					coupleMap.put(lineNo, new SimpleEntry<String, String>(f1NameFile, listOfFiles[j].getName()) );
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

	/**
	 * Loads on the HDFS the file pointed by the parameter
	 * @param path The path to a file on the local file system
	 */
	public void loadOnHDFS(String path){
		if (fs != null)
			throw new FileSystemNotFoundException("Trying to load on HDFS but the file system doesn't exist.");

		File toBeLoaded = null;
		try {
			fs = FileSystem.get(configuration);
			toBeLoaded = new File(path);
			writeOnHDFS(toBeLoaded);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	private int getHash(String s) {
		return s.hashCode();
	}


	public int getChecksum(String file){
		return checksums.get(file);
	}

//	public String getFileNameFromHash(String hash){
//		return inverseChecksums.get(hash);
//	}

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
		String s = null;
		
		openFS();
		
		
			try {
				stream = fs.create(toHdfs,true);
			} catch (IOException e) {
				logger.error("File not found: " + toHdfs.getName());
				e.printStackTrace();
			}
			br = new BufferedWriter(new OutputStreamWriter(stream));
			try {
				s = FileUtils.readFileToString(f);
			} catch (IOException e) {
				logger.error("File not found: " + toHdfs.getName());
				e.printStackTrace();
			}
			try {
				br.write(s);
			} catch (IOException e) {
				logger.error("File not found: " + toHdfs.getName());
				e.printStackTrace();
			}finally
			{
				try {
					if (br != null)
						br.close();
					
					if (stream != null)
						stream.close();
					
					closeFS();
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		
		
	}
	
	private void closeFS() {
		try {
			fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		fs = null;
	}

	public Map<String, Integer> getChecksums()
	{
		return checksums;
	}

	public void clean() {
		
	}
	
}
