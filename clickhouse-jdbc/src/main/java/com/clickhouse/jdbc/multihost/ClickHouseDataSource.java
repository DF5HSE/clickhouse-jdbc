package com.clickhouse.jdbc.multihost;

import com.clickhouse.jdbc.ClickHouseDriver;
import com.clickhouse.jdbc.JdbcWrapper;
import com.clickhouse.jdbc.internal.ClickHouseJdbcUrlParser;
import com.clickhouse.jdbc.internal.ClickHouseMultiHostUrlParser;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

class ClickHouseDataSource extends JdbcWrapper implements DataSource {
    private final String url;

    protected final ClickHouseDriver driver;
    protected final ClickHouseMultiHostUrlParser.ConnectionInfo connInfo;

    protected PrintWriter printWriter;
    protected int loginTimeoutSeconds = 0;

    public ClickHouseDataSource(String url) throws SQLException {
        this(url, new Properties());
    }

    public ClickHouseDataSource(String url, Properties properties) throws SQLException {
        if (url == null) {
            throw new IllegalArgumentException("Incorrect ClickHouse jdbc url. It must be not null");
        }
        this.url = url;

        this.driver = new ClickHouseDriver();
        this.connInfo = ClickHouseMultiHostUrlParser.parse(url, properties);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return null;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        printWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeoutSeconds = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeoutSeconds;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException(
                "Feature will be supported after merge with jdbc.ClickHouseDataSource"
        );
        /// return ClickHouseDriver.parentLogger;
    }
}
