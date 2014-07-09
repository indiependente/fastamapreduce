package simple.utils;


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

	private static BinRunner instance;
	private static Log logger = LogFactory.getLog(BinRunner.class);

	private BinRunner(){

	}

	public static BinRunner getInstance(){
		if (instance == null)
			instance = new BinRunner();
		return instance;
	}

	public int execute(String pathToBin, String pathToWorkingDir, List<String> args){
//		logger.info(pathToBin);
//		logger.info(pathToWorkingDir);

//		for (int i = 0; i<args.size(); i++){
//			logger.info(args.get(i));
//		}
//		
		if (pathToWorkingDir.endsWith("/"))
			pathToWorkingDir = pathToWorkingDir.substring(0, pathToWorkingDir.length()-1);
		
		args.add(0, pathToBin);
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
		System.out.println(filename);

		PrintWriter printWriter = null;
		try {
			printWriter = new PrintWriter(new File(filename));

			String line = null;

			try {
				while ((line = buffer.readLine()) != null) 
				{
					printWriter.println(line);

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
		return retValue;
	}

//	public static void main(String args[]){
//		BinRunner br = BinRunner.getInstance();
//		ArrayList<String> arrayList = new ArrayList<String>();
//		arrayList.add("mimmo");
//		arrayList.add("buddo");
//		br.execute("/Users/francescofarina/discovering/floatcond", "/Users/francescofarina/discovering/", arrayList);
//	}
}
