package flinkapp.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesLoader {

  private static final Logger LOG = LoggerFactory.getLogger(PropertiesLoader.class);

  /**
   * create java.util.Properties with the given configuration file
   * @param configProperty the system property to take configuration file name from
   * @param defaultConfigFile default filename to check in resources path
   * @return java.util.Properties Object loaded with configuration files if present, empty otherwise
   */
  public static Properties load(String configProperty, String defaultConfigFile) throws IOException {
    LOG.info("Loading configuration file...");

    if(System.getProperty(configProperty) != null) {
      LOG.info("Loading Configuration from "+configProperty+"="+System.getProperty(configProperty));
      return loadFromFile(System.getProperty(configProperty));
    } else {
      LOG.warn("No configuration property file defined. You can define it using system property: "+configProperty);
      // try to load config from default filename present in the same directory
      try {
        LOG.info("Searching for "+defaultConfigFile+" in current directory");
        return loadFromFile(defaultConfigFile);
      } catch (FileNotFoundException e){
        LOG.warn(defaultConfigFile+" not found in current directory, trying from resources");
      }
      try {
        return loadFromResource(defaultConfigFile);
      } catch (FileNotFoundException e){
        LOG.warn(defaultConfigFile+" not found in resources, returning empty Config");
        return new Properties();
      }
    }
  }

  public static Properties loadFromResource(String fileName) throws IOException {
    InputStream in = PropertiesLoader.class.getClassLoader().getResourceAsStream(fileName);

    if(in==null){
      throw new FileNotFoundException("property file "+fileName+" not found in classpath");
    }
    try {
      return load(in);
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        // do nothing
      }
    }
  }

  public static Properties loadFromFile(String fileName) throws IOException {
    try (InputStream in = new FileInputStream(fileName)) {
      return load(in);
    }
  }

  public static Properties load(InputStream inputStream) throws IOException {
    Properties properties = new Properties();
    properties.load(inputStream);
    return properties;
  }
}
