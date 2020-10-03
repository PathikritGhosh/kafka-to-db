package flinkapp.config;

import java.io.IOException;
import java.util.Map;

/**
 * Config class having connection properties and details of Kafka source and JDBC sink
 * Also contains details of kafka and jdbc fields that need to be tracked along with some Flink properties
 */
public class Configuration extends GenericConfig {

  public static final String CONFIGURATION_FILE = "flinkapp.conf";
  public static final ConfigDefinition DEFINITIONS = new ConfigDefinition();

  private static Configuration instance;

  public static final String SINK_DRIVER_NAME = "sink.driver.name";
  public static final String SINK_DB_URL = "sink.db.url";
  public static final String SINK_USERNAME = "sink.username";
  public static final String SINK_PASSWORD = "sink.password";
  public static final String SINK_TABLE = "sink.table";
  public static final String SINK_SCHEMA = "sink.schema";
  public static final String CHECKPOINTING_INTERVAL = "checkpointing.interval";
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  public static final String GROUP_ID = "group.id";
  public static final String TOPIC_NAME = "topic";
  public static final String KAFKA_FIELD_TYPES = "kafka.field.types";
  public static final String KAFKA_FIELD_NAMES = "kafka.field.names";
  public static final String SINK_COLUMN_TYPES = "sink.column.types";
  public static final String SINK_COLUMN_NAMES = "sink.column.names";
  public static final String INSERT_QUERY = "insert.query";

  static {
    DEFINITIONS
        .define(SINK_DRIVER_NAME, ConfigDefinition.Type.STRING, "com.mysql.jdbc.Driver")
        .define(SINK_DB_URL, ConfigDefinition.Type.STRING,"jdbc:mysql://****:3306/")
        .define(SINK_USERNAME, ConfigDefinition.Type.STRING,"***")
        .define(SINK_PASSWORD, ConfigDefinition.Type.STRING,"***")
        .define(SINK_SCHEMA, ConfigDefinition.Type.STRING,"db_schema")
        .define(SINK_TABLE, ConfigDefinition.Type.STRING, "flink_sink")
        .define(CHECKPOINTING_INTERVAL, ConfigDefinition.Type.INT, 60000)
        .define(BOOTSTRAP_SERVERS, ConfigDefinition.Type.STRING, "*****")
        .define(GROUP_ID, ConfigDefinition.Type.STRING, "flinkapp")
        .define(TOPIC_NAME, ConfigDefinition.Type.STRING, "flink_source")
        .define(KAFKA_FIELD_TYPES, ConfigDefinition.Type.LIST,
            "string, string, string")
        .define(KAFKA_FIELD_NAMES, ConfigDefinition.Type.LIST,
            "user, name, time_stamp")
        .define(SINK_COLUMN_TYPES, ConfigDefinition.Type.LIST,
            "string, string, string, string")
        .define(SINK_COLUMN_NAMES, ConfigDefinition.Type.LIST,
            "id, user, name, time_stamp")
        .define(INSERT_QUERY, ConfigDefinition.Type.STRING,
            "INSERT IGNORE INTO db_schema.flink_sink (id, user, name, time_stamp) VALUES (?, ?, ?, ?)");
  }

  public static Configuration getConf() throws IOException {
    if(instance != null) return instance;
    synchronized (Configuration.class){
      instance = new Configuration(DEFINITIONS, PropertiesLoader.loadFromResource(CONFIGURATION_FILE));
      return instance;
    }
  }

  private Configuration(ConfigDefinition definitions, Map<?, ?> originalConfigs) {
    super(definitions, originalConfigs);
  }
}