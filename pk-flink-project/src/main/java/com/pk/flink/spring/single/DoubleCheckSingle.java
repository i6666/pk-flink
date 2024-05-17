package com.pk.flink.spring.single;

/**
 * 双重检查
 * 缺点：序列化和反射可以破坏单例，需要额外处理。比如序列化时，可以重写 readResolve 方法，返回单例对象。
 * 反射通过 setAccessible(true)破坏单例
 */
public class DoubleCheckSingle {

    private DoubleCheckSingle() {
    }

    //volatile 关键字确保了当 instance 被构造函数初始化后，对 instance 的修改会立即对其他线程可见，从而防止了指令重排序的问题。
    private static volatile DoubleCheckSingle instance;

    public static DoubleCheckSingle getInstance() {
        if (instance == null) { //第一次检查 为了避免不必要的同步
            synchronized (DoubleCheckSingle.class) {
                if (instance == null) { //第二次检查 为了在null的情况下创建实例
                    instance = new DoubleCheckSingle(); //创建实例
                }
            }
        }
        return instance;
    }

}
