package redis.clients.jedis;

public class DebugParams {
  private String[] command;

  private DebugParams() {

  }
  
  public static DebugParams SLEEP(int seconds) {
      DebugParams debugParams = new DebugParams();
      debugParams.command = new String[] { "SLEEP", String.valueOf(seconds) };
      return debugParams;
    }

  public String[] getCommand() {
    return command;
  }
  
  public static DebugParams SEGFAULT() {
    DebugParams debugParams = new DebugParams();
    debugParams.command = new String[] { "SEGFAULT" };
    return debugParams;
  }

  public static DebugParams OBJECT(String key) {
    DebugParams debugParams = new DebugParams();
    debugParams.command = new String[] { "OBJECT", key };
    return debugParams;
  }

  public static DebugParams RELOAD() {
    DebugParams debugParams = new DebugParams();
    debugParams.command = new String[] { "RELOAD" };
    return debugParams;
  }
}
