package flinkapp.config;

import flinkapp.config.exception.ConfigException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenericConfig {

  private final Map<String,?> originalConfigs;

  private final Map<String, Object> values;

  @SuppressWarnings("unchecked")
  public GenericConfig(ConfigDefinition definitions, Map<?, ?> originalConfigs) {
    for(Object key : originalConfigs.keySet()){
      if(!(key instanceof String)){
        throw new ConfigException(key.toString(), originalConfigs.get(key), "Key must be a string.");
      }
    }
    this.originalConfigs = (Map<String,?>) originalConfigs;
    this.values=definitions.parse(originalConfigs);
  }

  public Object get(String key) {
    if (!values.containsKey(key))
      throw new ConfigException(String.format("Unknown configuration '%s'", key));
    return values.get(key);
  }

  public int getInt(String key) {
    return (Integer) get(key);
  }

  public long getLong(String key) {
    return (Long) get(key);
  }

  public double getDouble(String key) {
    return (Double) get(key);
  }

  public List<?> getList(String key) {
    return (List<?>) get(key);
  }

  public boolean getBoolean(String key) {
    return (Boolean) get(key);
  }

  public String getString(String key) {
    return (String) get(key);
  }

  public Class<?> getClass(String key) {
    return (Class<?>) get(key);
  }

  public Map<?, ?> getOriginalConfigs() {
    Map<String, Object> copy = new HashMap<String, Object>();
    copy.putAll(originalConfigs);
    return copy;
  }

  public Map<String, Object> getParsedConfigs() {
    return values;
  }

  public <T> T getConfiguredInstance(String key, Class<T> t) throws IllegalAccessException, InstantiationException {
    Class<?> c = getClass(key);
    if (c == null)
      return null;
    Object o = c.newInstance();
    if (!t.isInstance(o))
      throw new ConfigException(c.getName() + " is not an instance of " + t.getName());
    return t.cast(o);
  }

}