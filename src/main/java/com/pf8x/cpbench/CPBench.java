package com.pf8x.cpbench;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

public class CPBench {
    @State(Scope.Benchmark)
    public static class GState {
        @Param({"tomcat-jdbc", "HikariCP"})
        private String type;

        @Param({"10"})
        private int poolSize;

        private final String URL = "jdbc:mariadb://127.0.0.1:3306/bench";
        private final String DRIVER = "org.mariadb.jdbc.Driver";
        private final String USER = "bench";
        private final String PASSWORD = "123456";

        @Setup(Level.Trial)
        public void setup() {
            if(type.equals("HikariCP")) {
                HikariConfig config = new HikariConfig();
                config.setJdbcUrl(URL);
                config.setDriverClassName(DRIVER);
                config.setUsername(USER);
                config.setPassword(PASSWORD);
                config.setMaximumPoolSize(poolSize);
                config.setAutoCommit(false);

                ds = new HikariDataSource(config);
            }
            else if(type.equals("tomcat-jdbc")) {
                PoolProperties poolProperties = new PoolProperties();
                poolProperties.setUrl(URL);
                poolProperties.setDriverClassName(DRIVER);
                poolProperties.setUsername(USER);
                poolProperties.setPassword(PASSWORD);
                poolProperties.setInitialSize(poolSize);
                poolProperties.setMaxActive(poolSize);
                poolProperties.setMaxIdle(poolSize);
                poolProperties.setFairQueue(true);
                poolProperties.setDefaultAutoCommit(false);
                ds = new org.apache.tomcat.jdbc.pool.DataSource(poolProperties);
            }
        }

        @TearDown(Level.Trial)
        public void teardown() {
            if(type.equals("HikariCP")) ((HikariDataSource) ds).close();
            else if(type.equals("tomcat-jdbc")) ((org.apache.tomcat.jdbc.pool.DataSource) ds).close();
        }

        public DataSource ds;
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime, Mode.SampleTime, Mode.Throughput }) @OutputTimeUnit(TimeUnit.MICROSECONDS)
    @Warmup(iterations = 1)
    @Measurement(iterations = 1)
    @Threads(32)
    public void benchStatementCycle(GState gs, Blackhole bh) throws SQLException {
        Connection con = gs.ds.getConnection();
        Statement stmt = con.createStatement();
        bh.consume(stmt.executeQuery("SELECT id FROM test WHERE id = 1"));
        stmt.close();
        con.close();
    }

    public static void main(String[] args) throws RunnerException, SQLException {
        Options options = new OptionsBuilder().include(CPBench.class.getSimpleName()).forks(1).build();
        new Runner(options).run();
    }
}
