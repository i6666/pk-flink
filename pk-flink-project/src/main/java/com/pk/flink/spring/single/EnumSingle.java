package com.pk.flink.spring.single;

/**
 * 枚举单例
 * 优点：线程安全，防止反射和序列化破坏
 */
public class EnumSingle {
    private EnumSingle() {
    }
    public static EnumSingle getInstance() {
        return Singleton.INSTANCE.getInstance();
    }

    /**
     * 枚举单例
     */
    private enum Singleton {
        INSTANCE;
        private EnumSingle instance;
        Singleton() {
            instance = new EnumSingle();
        }
        public EnumSingle getInstance() {
            return instance;
        }
    }
}
