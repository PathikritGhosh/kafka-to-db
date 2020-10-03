package flinkapp.util;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.log4j.Logger;

import static flinkapp.util.JSONConstants.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;


/**
 * Class to extract event timestamp
 * Allows for watermarking based on event time
 */
public class TimeStampAssigner implements AssignerWithPeriodicWatermarks<ObjectNode> {

  private static final Logger LOG = Logger.getLogger(TimeStampAssigner.class);
  private final long maxOutOfOrder = 1000 * 60;

  private long currentMaxTimestamp;

  @Override
  public long extractTimestamp(ObjectNode objectNode, long previousElementTimestamp) {

    String timeStamp = objectNode.get(JSON_VALUE).get("time_stamp").textValue();
    DateTimeFormatter parseFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    LocalDateTime dateTime = LocalDateTime.parse(timeStamp, parseFormatter);
    long epochTimestamp = dateTime.atOffset(ZoneOffset.UTC).toInstant().toEpochMilli();

    currentMaxTimestamp = Math.max(epochTimestamp, currentMaxTimestamp);
    LOG.debug("Extracted event timestamp: " + currentMaxTimestamp);
    return epochTimestamp;
  }

  @Override
  public Watermark getCurrentWatermark() {
    return new Watermark(currentMaxTimestamp - maxOutOfOrder);
  }
}
