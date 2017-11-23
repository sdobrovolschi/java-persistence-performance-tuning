package com.example.jdbc;

import com.zaxxer.hikari.HikariDataSource;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * @author Stanislav Dobrovolschi
 */
@State(Scope.Benchmark)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BatchPreparedStatementBenchmark {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BatchPreparedStatementBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Param({"10", "20", "30", "40", "50", "60", "70", "80", "90", "100", "500"})
    private int batchSize;

    @Benchmark
    @CompilerControl(CompilerControl.Mode.INLINE)
    public void transactionCycle(ConnectionState context, Blackhole blackhole) throws SQLException {
        PreparedStatement preparedStatement = context.connection.prepareStatement("insert into project(id, name) values (?, ?)");

        for (int i = 1; i <= 5000; i++) {
            preparedStatement.setInt(1, i);
            preparedStatement.setString(2, "Customer" + i);
            preparedStatement.addBatch();

            if (i % batchSize == 0) {
                preparedStatement.executeBatch();
                preparedStatement.clearBatch();
            }
        }

        preparedStatement.executeBatch();

        preparedStatement.close();
        blackhole.consume(preparedStatement);
    }

    @State(Scope.Thread)
    public static class ConnectionState {

        @Param({"jdbc:oracle:thin:@localhost:1521:orcl",
                "jdbc:sqlserver://localhost:1433;databaseName=test",
                "jdbc:postgresql://localhost:5432/test",
                "jdbc:mysql://localhost:3306/test?useSSL=false&rewriteBatchedStatements=true"})
        private String url;

        @Param("xmczpjkntr")
        private String username;

        @Param("wJmx2GEKVp")
        private String password;

        private int maxPoolSize = 5;

        private DataSource dataSource;
        private Connection connection;

        @Setup
        public void setUpDataSource() {
            HikariDataSource hikariDataSource = new HikariDataSource();
            hikariDataSource.setJdbcUrl(url);
            hikariDataSource.setUsername(username);
            hikariDataSource.setPassword(password);
            hikariDataSource.setMinimumIdle(maxPoolSize);
            hikariDataSource.setMaximumPoolSize(maxPoolSize);
            dataSource = hikariDataSource;
        }

        @TearDown
        public void cleanUpDataSource() throws SQLException {
            ((HikariDataSource) dataSource).close();
        }

        @Setup(Level.Invocation)
        public void setUpConnection() throws SQLException {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
        }

        @TearDown(Level.Invocation)
        public void cleanUpConnection() throws SQLException {
            connection.rollback();
            connection.close();
        }
    }
}
