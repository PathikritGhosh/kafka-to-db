package flinkapp;


import flinkapp.config.Configuration;
import flinkapp.mappers.InputMapper;
import flinkapp.util.JDBCUtil;
import flinkapp.util.TimeStampAssigner;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;


import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * A Generic configurable Flink app, which takes source data from Kafka topic and
 * dumps the selected columns to a JDBC sink
 * Achieves exactly-once ingestion by avoiding duplicate insertions on the JDBC sink
 */

public class Main {

  private final Configuration conf;
  private static final Logger LOG = Logger.getLogger(Main.class);

  /**
   * Initialize config class
   * @throws IOException thrown by Configuration class
   */
  private Main() throws IOException {
    this.conf = Configuration.getConf();
    LOG.info("Configuration initialized");
  }

  public static void main(String[] args) throws Exception {
    new Main().start();
  }

  /**
   * Initializing the source and sink
   * Setting up the DataStream pipeline
   * trigger the program execution for this job
   * @throws Exception thrown by execute()
   */
  private void start() throws Exception {
    StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    flinkEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    flinkEnv.enableCheckpointing(conf.getInt(Configuration.CHECKPOINTING_INTERVAL), CheckpointingMode.EXACTLY_ONCE);

    Properties props = new Properties();
    props.setProperty("bootstrap.servers", conf.getString(Configuration.BOOTSTRAP_SERVERS));
    props.setProperty("group.id", conf.getString(Configuration.GROUP_ID));

    FlinkKafkaConsumer011<ObjectNode> kafkaSource =
        new FlinkKafkaConsumer011<>(conf.getString(Configuration.TOPIC_NAME), new JSONKeyValueDeserializationSchema(true), props);
    kafkaSource.assignTimestampsAndWatermarks(new TimeStampAssigner());
    DataStream<ObjectNode> stream = flinkEnv.addSource(kafkaSource);

    LOG.info("Flink Kafka consumer initialized along with event-based watermarking support");

    DataStream<Row> rowStream = stream.map(new InputMapper(
            (List<String>)conf.getList(Configuration.KAFKA_FIELD_NAMES),
            (List<String>)conf.getList(Configuration.KAFKA_FIELD_TYPES)
    )).uid("Input Mapper").name("Input Mapper");

    LOG.info("Mapper to transform data put in the data pipeline");


    TypeInformation[] FIELD_TYPES = JDBCUtil.getSinkTypes((List<String>)conf.getList(Configuration.SINK_COLUMN_TYPES));
    String query = conf.getString(Configuration.INSERT_QUERY);

    LOG.info("Query to be executed on sink: " + query);


    JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
        .setDrivername(conf.getString(Configuration.SINK_DRIVER_NAME))
        .setDBUrl(conf.getString(Configuration.SINK_DB_URL))
        .setUsername(conf.getString(Configuration.SINK_USERNAME))
        .setPassword(conf.getString(Configuration.SINK_PASSWORD))
        .setQuery(query)
        .setParameterTypes(FIELD_TYPES)
        .build();
    sink.consumeDataStream(rowStream).uid("MySQL Sink").name("MySQL Sink");

    LOG.info("Sink initialized");

    flinkEnv.execute("Executing Flink-to-jdbc job ...");
  }

}
