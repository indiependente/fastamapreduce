package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ConfigurationLoader {

	public static final String BLOCK_SIZE = "dfs.blocksize";
	
	public static final String HADOOP_HOME = "/";
	
	private static final String[] configFilesToLoad = { "core-site.xml", "hdfs-site.xml" };
	
	private Configuration conf;
	
	private ConfigurationLoader()
	{
		conf = new Configuration();
		for (String s : configFilesToLoad)
			conf.addResource(new Path(HADOOP_HOME + s));
	}
	
	public Configuration getConfiguration()
	{
		return conf;
	}
	
	public String getValue(String key)
	{
		return conf.get(key);
	}
	
	public int getIntValue(String key)
	{
		return Integer.getInteger(getValue(key));
	}
	
	private static ConfigurationLoader instance = null;
	public static ConfigurationLoader getInstance()
	{
		if (instance == null)
			instance = new ConfigurationLoader();
		return instance;
	}

	
}
