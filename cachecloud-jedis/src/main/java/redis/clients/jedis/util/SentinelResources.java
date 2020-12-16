package redis.clients.jedis.util;

/**
 * @author wenruiwu
 * @create 2020/12/1 11:55
 * @description
 */
public class SentinelResources {

    /**
     * 定义crossroom sentinel resource
     */
    public static final String MAJOR_READ_COMMAND_KEY = "major_read_command";
    public static final String MAJOR_WRITE_COMMAND_KEY = "major_write_command";
    public static final String MINOR_READ_COMMAND_KEY = "minor_read_command";
    public static final String MINOR_WRITE_COMMAND_KEY = "minor_write_command";
}
