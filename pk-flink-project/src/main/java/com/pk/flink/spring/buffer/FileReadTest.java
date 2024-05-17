package com.pk.flink.spring.buffer;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Warmup(iterations = 0, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations =1, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
public class FileReadTest {


//    @Benchmark
    public void read() throws IOException {
        // 1. 创建一个文件对象
        // 2. 创建一个文件输入流
        // 4. 读取文件内容
        // 5. 关闭流
        FileReader reader = new FileReader("/Users/strong/workspace/flink/pk-flink/pk-flink-project/src/main/resources/test.txt");
        StringBuilder readTxt = new StringBuilder();
        while (reader.read() != -1) {
            readTxt.append((char) reader.read());
        }
//        System.err.println(readTxt);

    }

    @Benchmark
    public void bufferRead() throws IOException {
        //装饰器模式，增强FileReader功能
        BufferedReader bufferedReader = new BufferedReader(new FileReader("/Users/strong/workspace/flink/pk-flink/pk-flink-project/src/main/resources/test.txt"));
        String line = null;
        StringBuilder readTxt = new StringBuilder();
        while ((line = bufferedReader.readLine()) != null) {
            readTxt.append(line);
        }
//        System.err.println(readTxt);
    }



    public static void main(String[] args) throws RunnerException {
        Options opts = new OptionsBuilder()
                .include(FileReadTest.class.getSimpleName())
                .resultFormat(ResultFormatType.JSON)
                .build();
        new Runner(opts).run();
    }
}
