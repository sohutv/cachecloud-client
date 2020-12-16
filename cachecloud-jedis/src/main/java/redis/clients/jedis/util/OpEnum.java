package redis.clients.jedis.util;

/**
 * @author wenruiwu
 * @create 2020/11/30 10:28
 * @description
 */
public enum OpEnum {

    READ(1, "read operation"),
    WRITE(2, "write operation");

    private int type;

    private String info;

    private OpEnum(int type, String info) {
        this.type = type;
        this.info = info;
    }

    public int getType() {
        return type;
    }

    public String getInfo() {
        return info;
    }

}
