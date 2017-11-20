package com.example.jdbc;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * @author Stanislav Dobrovolschi
 */
@Warmup(iterations = 15)
@Measurement(iterations = 15)
@Threads(30)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ConnectionPoolBenchmark {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ConnectionPoolBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    @CompilerControl(CompilerControl.Mode.INLINE)
    public void connectionCycle(BenchmarkContext context, Blackhole blackhole) throws SQLException {
        Connection connection = context.dataSource.getConnection();
        connection.close();
        blackhole.consume(connection);
    }

    @State(Scope.Benchmark)
    public static class BenchmarkContext {

        @Param({"hikariCP", "tomcat", "dbcp2", "c3p0"})
        private String pool;

        private String url = "jdbc:sqlserver://localhost:1433;databaseName=test";
        private String username = "sa";
        private String password = "@passw0rd@";
        private int maxPoolSize = 5;

        private DataSource dataSource;

        @Setup
        public void setUp() {
            switch (pool) {
                case "hikariCP":
                    this.dataSource = hikariCP();
                    break;
                case "tomcat":
                    this.dataSource = tomcat();
                    break;
                case "dbcp2":
                    this.dataSource = dbcp2();
                    break;
                case "c3p0":
                    this.dataSource = c3p0();
                    break;
            }
        }

        @TearDown
        public void cleanUp() throws SQLException {
            switch (pool) {
                case "hikariCP":
                    ((HikariDataSource) dataSource).close();
                    break;
                case "tomcat":
                    ((org.apache.tomcat.jdbc.pool.DataSource) dataSource).close();
                    break;
                case "dbcp2":
                    ((BasicDataSource) dataSource).close();
                    break;
                case "c3p0":
                    ((ComboPooledDataSource) dataSource).close();
                    break;
            }
        }

        private DataSource hikariCP() {
            HikariDataSource dataSource = new HikariDataSource();
            dataSource.setJdbcUrl(url);
            dataSource.setUsername(username);
            dataSource.setPassword(password);
            dataSource.setMinimumIdle(maxPoolSize);
            dataSource.setMaximumPoolSize(maxPoolSize);

            return dataSource;
        }

        private DataSource dbcp2() {
            BasicDataSource dataSource = new BasicDataSource();
            dataSource.setUrl(url);
            dataSource.setUsername(username);
            dataSource.setPassword(password);
            dataSource.setInitialSize(maxPoolSize);
            dataSource.setMaxIdle(maxPoolSize);
            dataSource.setMaxTotal(maxPoolSize);

            return dataSource;
        }

        private DataSource tomcat() {
            org.apache.tomcat.jdbc.pool.DataSource dataSource = new org.apache.tomcat.jdbc.pool.DataSource();
            dataSource.setUrl(url);
            dataSource.setUsername(username);
            dataSource.setPassword(password);
            dataSource.setInitialSize(maxPoolSize);
            dataSource.setMaxIdle(maxPoolSize);
            dataSource.setMaxActive(maxPoolSize);

            return dataSource;
        }

        private DataSource c3p0() {
            ComboPooledDataSource dataSource = new ComboPooledDataSource();
            dataSource.setJdbcUrl(url);
            dataSource.setUser(username);
            dataSource.setPassword(password);
            dataSource.setInitialPoolSize(maxPoolSize);
            dataSource.setMaxPoolSize(maxPoolSize);

            return dataSource;
        }
    }
}
