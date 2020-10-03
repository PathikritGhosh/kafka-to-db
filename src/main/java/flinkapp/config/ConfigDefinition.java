package flinkapp.config;

import flinkapp.config.exception.ConfigException;

import java.util.*;

public class ConfigDefinition {

  private static final Object NO_DEFAULT_VALUE ="";

  private final Map<String,Config> configMap =  new HashMap<String, Config>();

  private ConfigDefinition define(String name, Type type, Object defaultValue, Validator validator, boolean required) {
    if(configMap.containsKey(name)){
      throw new ConfigException("configuration "+name+" has been defined more than once.");
    }
    Object parsedDefault = defaultValue == NO_DEFAULT_VALUE ? NO_DEFAULT_VALUE : parseType(name, defaultValue, type);
    configMap.put(name, new Config(name,type,parsedDefault,validator,required));
    return this;
  }


  public ConfigDefinition define(String name, Type type, Validator validator){
    return define(name,type,NO_DEFAULT_VALUE,validator,true);
  }

  public ConfigDefinition define(String name, Type type){
    return define(name,type,NO_DEFAULT_VALUE,null,true);
  }

  public ConfigDefinition define(String name, Type type, Object defaultValue){
    return define(name,type,defaultValue,null,false);
  }

  public ConfigDefinition define(String name, Type type,Object defaultValue, Validator validator ){
    return define(name,type,defaultValue,validator,false);
  }

  /**
   *
   * @param actualValues The map of original configuration values passed
   * @return the map of validated and parsed configuration values
   */

  public Map<String, Object> parse(Map<?,?> actualValues){

    Map<String, Object> values = new HashMap<String, Object>();

    for(Config config : configMap.values()){
      Object value;
      if(System.getProperty(config.name)!=null)
        value = parseType(config.name,System.getProperty(config.name),config.type);
      else if (actualValues.containsKey(config.name))
        value = parseType(config.name, actualValues.get(config.name), config.type);
      else if (config.defaultValue == NO_DEFAULT_VALUE && config.required)
        throw new ConfigException("Missing required configuration \"" + config.name + "\" which has no default value.");
      else
        value = config.defaultValue;
      if (config.validator != null)
        config.validator.validate(config.name, value);
      values.put(config.name, value);
    }
    return values;
  }

  /**
   *
   * @param name name of the configuration property
   * @param value value of the config
   * @param type expected type
   * @return the value parsed to its Type
   */
  public Object parseType(String name, Object value, Type type){
    String trimmedValue=null;
    if(value instanceof String)
      trimmedValue = ((String) value).trim();

    try {
      switch (type){
        case BOOLEAN:
          if(value instanceof String){
            if(trimmedValue.equalsIgnoreCase("true"))
              return true;
            else if(trimmedValue.equalsIgnoreCase("false"))
              return false;
            else
              throw new ConfigException(name,value,"value must of type "+type);
          }
          return (Boolean) value;
        case INT:
          if(value instanceof String)
            return Integer.parseInt(trimmedValue);
          return (Integer) value;

        case LONG:
          if(value instanceof Integer)
            return ((Integer) value).longValue();
          if(value instanceof String){
            if (trimmedValue.substring(trimmedValue.length() - 1).equalsIgnoreCase("l"))
              trimmedValue = trimmedValue.substring(0, trimmedValue.length() - 1);
            return Long.parseLong(trimmedValue);
          }
          return (Long) value;

        case DOUBLE:
          if (value instanceof Number)
            return ((Number) value).doubleValue();
          if(value instanceof String)
            return Double.parseDouble(trimmedValue);
          return (Double) value;

        case STRING:
          if(value instanceof String)
            return trimmedValue;
          return value.toString();
        case LIST:
          if(value instanceof String){
            if(trimmedValue.length()==0)
              return Collections.emptyList();
            else
              return Arrays.asList(trimmedValue.split("\\s*,\\s*", -1));
          }
          if(value instanceof List)
            return (List) value;
          else
            throw new ConfigException(name,value,"Expected a comma separated list");
        case CLASS:
          if(value instanceof String)
            return Class.forName(trimmedValue);
          return (Class) value;
        default:
          throw new IllegalStateException("unknown type");
      }
    }catch (NumberFormatException e){
      throw new ConfigException(name,value,"value must be of type "+type);
    }catch (ClassCastException e){
      throw new ConfigException(name,value,"value must be of type "+type);
    } catch (ClassNotFoundException e) {
      throw new ConfigException(name,value,"Class "+value+" not found");
    }

  }


  public enum Type {
    BOOLEAN,INT,LONG,DOUBLE,STRING,LIST,CLASS
  }

  public interface Validator {
    public void validate(String name, Object value);
  }

  public static class Range implements Validator {

    private Number min;
    private Number max;

    public Range(Number min, Number max) {
      this.min = min;
      this.max = max;
    }

    public static Range atLeast(Number min){
      return new Range(min,null);
    }

    public static Range atMost(Number max){
      return new Range(null,max);
    }

    public static Range between(Number min, Number max){
      return new Range(min,max);
    }

    @Override
    public void validate(String name, Object value) {
      Number n = (Number) value;

      if(value instanceof Double || value instanceof Float){
        if(this.min !=null && n.doubleValue() < this.min.doubleValue()){
          throw new ConfigException(name,value,"Value must be at least "+this.min);
        }
        if( this.max!=null && n.doubleValue() > this.max.doubleValue()){
          throw new ConfigException(name,value,"Value must not be more than "+this.max);
        }
      } else {
        if(this.min !=null && n.longValue() < this.min.longValue()){
          throw new ConfigException(name,value,"Value must be at least "+this.min);
        }
        if( this.max!=null && n.longValue() > this.max.longValue()){
          throw new ConfigException(name,value,"Value must not be more than "+this.max);
        }
      }
    }

  }

  public static class ValidString implements Validator {

    private String regex;

    public ValidString(String regex) {
      this.regex = regex;
    }

    public static ValidString matching(String regex){
      return new ValidString(regex);
    }

    @Override
    public void validate(String name, Object value) {
      String s = (String) value;
      if(!s.matches(regex)){
        throw new ConfigException(name,value);
      }
    }
  }

  public static class SetValidator<T> implements Validator {

    private List<T> validValues;

    public SetValidator(List<T> validValues) {
      this.validValues = validValues;
    }

    public void validate(String name, Object value) {
      if(!this.validValues.contains(value)){
        throw new ConfigException(name,value,"valid values are only "+this.validValues);
      }
    }

    public static SetValidator<String> in(String... validValues){
      return new SetValidator<String>(Arrays.asList(validValues));
    }

    public static SetValidator<Number> in(Number... validValues){
      return new SetValidator<Number>(Arrays.asList(validValues));
    }

    public static <T> SetValidator<T> in(T... validValues){
      return new SetValidator<T>(Arrays.asList(validValues));
    }
  }

  public class Config {
    public final String name;
    public final Type type;
    public final Object defaultValue;
    public final Validator validator;
    public final boolean required;

    public Config(String name, Type type, Object defaultValue, Validator validator, boolean required) {
      this.name = name;
      this.type = type;
      this.defaultValue = defaultValue;
      this.validator = validator;
      this.required = required;
    }
  }

}