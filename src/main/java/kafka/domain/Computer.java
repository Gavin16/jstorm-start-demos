package kafka.domain;

/**
 * 实体类测试序列化 和 反序列化
 */
public class Computer {

    private String type;

    private String cpu;

    private String memory;

    private String capacity;

    private Integer price;

    public Computer(){}

    public Computer(String type,String cpu,String memory,String capacity,Integer price){
        this.type = type;
        this.cpu = cpu;
        this.memory = memory;
        this.capacity = capacity;
        this.price = price;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getCpu() {
        return cpu;
    }

    public void setCpu(String cpu) {
        this.cpu = cpu;
    }

    public String getMemory() {
        return memory;
    }

    public void setMemory(String memory) {
        this.memory = memory;
    }

    public String getCapacity() {
        return capacity;
    }

    public void setCapacity(String capacity) {
        this.capacity = capacity;
    }

    public Integer getPrice() {
        return price;
    }

    public void setPrice(Integer price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "Computer{" +
                "type='" + type + '\'' +
                ", cpu='" + cpu + '\'' +
                ", memory='" + memory + '\'' +
                ", capacity='" + capacity + '\'' +
                ", price=" + price +
                '}';
    }
}
