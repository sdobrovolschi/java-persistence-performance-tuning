package com.example.jdbc;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * @author Stanislav Dobrovolschi
 */
@Warmup(iterations = 15)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class ConnectionAcquisitionBenchmark {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ConnectionAcquisitionBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @State(Scope.Benchmark)
    public static class BenchmarkContext {

        @Param({"jdbc:oracle:thin:@localhost:1521:orcl",
                "jdbc:sqlserver://localhost:1433;databaseName=test",
                "jdbc:postgresql://localhost:5432/test",
                "jdbc:mysql://localhost:3306/test?useSSL=false"})
        public String url;

        @Param("xmczpjkntr")
        public String username;

        @Param("wJmx2GEKVp")
        public String password;
    }

    @Benchmark
    public void connectionCycle(BenchmarkContext context, Blackhole blackhole) throws SQLException {
        Connection connection = DriverManager.getConnection(context.url, context.username, context.password);
        connection.close();
        blackhole.consume(connection);
    }

}
