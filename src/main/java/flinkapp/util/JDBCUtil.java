package flinkapp.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;

/**
 * JDBC utilities class
 */
public class JDBCUtil {

  private static final Logger LOG = Logger.getLogger(JDBCUtil.class);

  /**
   * Map of data types to Flink data types
   */
  private static final HashMap<String , TypeInformation> DATA_TYPE_MAP = new HashMap<String , TypeInformation>() {{
    put("string", Types.STRING);
    put("int", Types.INT);
    put("long", Types.LONG);
    put("double", Types.DOUBLE);
    put("boolean", Types.BOOLEAN);
    put("decimal", Types.BIG_DEC);
  }};

  /**
   * return type information of the row to be inserted in Flink sink
   * @param dataTypes data types as put in the configuration
   * @return Flink type information for each JDBC row in sink
   */
  public static TypeInformation[] getSinkTypes(List<String> dataTypes) {

    int totalColumns = dataTypes.size();
    TypeInformation[] typesArray = new TypeInformation[totalColumns];

    for(int columnCounter = 0; columnCounter < totalColumns; columnCounter++) {
      typesArray[columnCounter] = DATA_TYPE_MAP.get(dataTypes.get(columnCounter));
    }
    return typesArray;
  }

}
