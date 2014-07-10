package utils;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class BinRunner {

	private static Log logger = LogFactory.getLog(BinRunner.class);


	/**
	 * Static method that executes a binary executable file in a given working directory. Takes the list of argument too.
	 * @param pathToBin Path to the binary file
	 * @param pathToWorkingDir Path to the working directory
	 * @param args Arguments to be supplied at the binary file
	 * @param runnable 
	 * @return The absolute path to the output file
	 * @throws IOException
	 */
	public static String execute(String pathToBin, String pathToWorkingDir, List<String> args, Runnable runnable) throws IOException{
		//		logger.info(pathToBin);
		//		logger.info(pathToWorkingDir);

		//		for (int i = 0; i<args.size(); i++){
		//			logger.info(args.get(i));
		//		}
		//		
		File binFile = new File(pathToBin);
		if (!binFile.exists())
			throw new FileNotFoundException("File not found: "+pathToBin);
		if (!binFile.canExecute())
			throw new IOException("Cannot execute program: "+pathToBin);

		if (pathToWorkingDir.endsWith("/"))
			pathToWorkingDir = pathToWorkingDir.substring(0, pathToWorkingDir.length()-1);

//		args.add(0, pathToBin);
		ProcessBuilder runner = new ProcessBuilder(args);
		runner.directory(new File(pathToWorkingDir));
		runner.redirectErrorStream(true);
		logger.info("running...");


		Process p = null;
		try {
			p = runner.start();
		} catch (IOException e) {
			e.printStackTrace();
		}

		InputStream stdin = p.getInputStream();
		InputStreamReader isr = new InputStreamReader(stdin);
		BufferedReader buffer = new BufferedReader(isr);

		String filename = pathToWorkingDir + pathToBin.substring(pathToBin.lastIndexOf("/"), pathToBin.length())+"_"+(new Date()).toString().replaceAll(" ", "_")+".out";

		PrintWriter printWriter = null;
		try {
			printWriter = new PrintWriter(new File(filename));

			String line = null;

			try {
				while ((line = buffer.readLine()) != null) 
				{
					printWriter.println(line);
					runnable.run();
				}
			}catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		int retValue = 0;
		try {
			retValue = p.waitFor(); // it returns the exit value..
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			printWriter.close();
		}
		return filename;
	}

	//	public static void main(String args[]){
	//		ArrayList<String> arrayList = new ArrayList<String>();
	//		arrayList.add("mimmo");
	//		arrayList.add("buddo");
	//		BinRunner.execute("/Users/francescofarina/discovering/floatcond", "/Users/francescofarina/discovering/", arrayList);
	//	}
}
