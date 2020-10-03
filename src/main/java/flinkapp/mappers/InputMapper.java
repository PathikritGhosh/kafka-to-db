package flinkapp.mappers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static flinkapp.util.JSONConstants.*;

/**
 * Mapper to convert the JSON received from kafka topic into
 * "Row" class that is needed to insert into JDBCSink
 * Extracts only the needed fields from Kafka topic, as specified in the configuration
 * Appends a key using a combination of Kafka topic partition and offset to get a unique key id for each message
 */
public class InputMapper implements MapFunction<ObjectNode, Row> {

  private static final Logger LOG = Logger.getLogger(InputMapper.class);
  private List<String> fieldNames;
  private List<String> fieldTypes;

  /**
   * Constructor
   * @param fieldNames field to be extracted from Kafka topic
   * @param fieldTypes data type of the corresponding fields
   */
  public InputMapper(List<String> fieldNames, List<String> fieldTypes) {
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
  }

  /**
   * Map function that needs to be implemented
   * @param data Input JSON data that we get from Kafka source
   * @return Row format data that is put into the sink
   */
  @Override
  public Row map(ObjectNode data) {

    String keyId = data.get(JSON_METADATA).get(JSON_TOPIC) + "_" + data.get(JSON_METADATA).get(JSON_PARTITION) + "_" +
        data.get(JSON_METADATA).get(JSON_OFFSET);

    LOG.debug("Unique key of the row to be inserted: "+ keyId);

    return Row.of(getFields(data, keyId).toArray());
  }

  /**
   *
   * @param data JSON data got from Kafka
   * @param keyId unique key formed by appending Kafka topic offset to Kafka topic and partition
   * @return a list of objects that holds kafka fields
   */
  private List<Object> getFields(ObjectNode data, String keyId) {
    List<Object> fieldList = new ArrayList<>();
    fieldList.add(keyId);

    int arrSize = fieldNames.size();
    for(int i = 0; i < arrSize; ++i) {
      fieldList.add(getValue(data, fieldNames.get(i), fieldTypes.get(i)));
    }

    LOG.debug("Row to be inserted: " + fieldList);

    return fieldList;
  }

  /**
   * Function to extract a field from JSON data according to the data type of the field
   * @param data JSON data
   * @param fieldName  field to be extracted
   * @param fieldType data type of the field
   * @return Extracted field returned as its parent class
   */
  private Object getValue(ObjectNode data, String fieldName, String fieldType) {

    JsonNode field = data.get(JSON_VALUE).get(fieldName);

    switch (fieldType) {
      case "string":
        return field.asText(null);
      case "int":
        return field.asInt();
      case "long":
        return field.asLong();
      case "double":
        return field.asDouble();
      case "boolean":
        return field.asBoolean();
      default:
        LOG.error("Unreachable type --" + fieldType + "-- for name --" + fieldName);
        break;
    }

    return  null;
  }

}
