package com.clickhouse.jdbc.internal;

import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;
import com.clickhouse.jdbc.JdbcConfig;
import com.clickhouse.jdbc.SqlExceptionUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;


public class ClickHouseMultiHostUrlParser {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseMultiHostUrlParser.class);

    public static class ConnectionInfo {
        private final String URIRawQuery;
        private final ClickHouseNode[] servers;
        private final JdbcConfig jdbcConf;
        private final Properties props;

        protected ConnectionInfo(String URIRawQuery, ClickHouseNode[] servers, Properties props) throws URISyntaxException {
            this.URIRawQuery = URIRawQuery;
            this.servers = servers;
            this.jdbcConf = new JdbcConfig(props);
            this.props = props;
        }

        public URI getUriByNode(ClickHouseNode server) throws URISyntaxException {
            return new URI("jdbc:clickhouse:" + server.getProtocol().name().toLowerCase(Locale.ROOT), null,
                    server.getHost(), server.getPort(), "/" + server.getDatabase().orElse(""),
                    ClickHouseJdbcUrlParser.removeCredentialsFromQuery(URIRawQuery), null);
        }
        public ClickHouseNode[] getServers() {
            return servers;
        }

        public JdbcConfig getJdbcConf() {
            return jdbcConf;
        }

        public Properties getProps() {
            return props;
        }
    }

    public static ConnectionInfo parse(String jdbcUrl, Properties defaults) throws SQLException {
        String[] partsDoubleSlash = jdbcUrl.split("//");
        if (partsDoubleSlash.length == 1) {
            throw SqlExceptionUtils
                    .clientError(new URISyntaxException(jdbcUrl, "Missing '//' from the given JDBC URL"));
        }

        String[] partsSlashAfterHost = partsDoubleSlash[1].split("/", 2);
        if (partsSlashAfterHost.length == 1) {
            throw SqlExceptionUtils
                    .clientError(new URISyntaxException(jdbcUrl, "Missing '/' after host or port"));
        }

        String[] authorityParts = partsSlashAfterHost[0].split("@");
        if (authorityParts.length > 2) {
            throw SqlExceptionUtils
                    .clientError(new URISyntaxException(jdbcUrl, "Too much '@' in authority"));
        }

        String userInfo = "", hosts = "";
        if (authorityParts.length == 2) {
            userInfo = authorityParts[0] + "@";
            hosts = authorityParts[1];
        } else {
            hosts = authorityParts[0];
        }
        if (hosts.isEmpty()) {
            throw SqlExceptionUtils
                    .clientError(new URISyntaxException(jdbcUrl, "URL doesn't contain host info"));
        }
        String[] hosts_ports = hosts.split(",");

        String URIRawQuery = "";
        List<ClickHouseNode> servers = new ArrayList<>();
        Properties properties = null;
        try {
            for (String host_port : hosts_ports) {
                String oneHostUrl = partsDoubleSlash[0] + "//" + userInfo + host_port + "/" + partsSlashAfterHost[1];
                ClickHouseJdbcUrlParser.ConnectionInfo ci = ClickHouseJdbcUrlParser.parse(oneHostUrl, defaults);
                URIRawQuery = ci.getUri().getRawQuery();
                servers.add(ci.getServer());
                properties = ci.getProperties();
            }
            return new ConnectionInfo(URIRawQuery, servers.toArray(new ClickHouseNode[0]), properties);
        } catch (URISyntaxException | IllegalArgumentException e) {
            throw SqlExceptionUtils.clientError(e);
        }
    }
}