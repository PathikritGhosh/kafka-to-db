package flinkapp.config.exception;

public class ConfigException extends RuntimeException {

  public ConfigException(String message) {
    super(message);
  }

  public ConfigException(String name, Object value) {
    this(name, value, null);
  }

  public ConfigException(String name, Object value, String message) {
    super("Invalid value " + value + " for configuration " + name + (message == null ? "" : ": " + message));
  }

}