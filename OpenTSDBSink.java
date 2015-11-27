package org.apache.hadoop.metrics2.sink;


import org.apache.commons.compress.utils.Charsets;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.util.Servers;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;

/**
 * A metrics sink that writes to a OpenTSDB server
 */
public class OpenTSDBSink implements MetricsSink, Closeable {

  private final static Log LOG = LogFactory.getLog(OpenTSDB.class);
  private static OpenTSDB openTSDB = null;
  private static final int defaultPort = 4242;
  private List<? extends SocketAddress> TSDBServers;
  private String TSDBEntry = "put %s %d %s %s\n";

  @Override
  public void init(SubsetConfiguration conf) {
    this.TSDBServers = Servers.parse(conf.getString("servers"), defaultPort);
    Random random = new Random();
    int i = random.nextInt(this.TSDBServers.size());
    InetSocketAddress address = (InetSocketAddress) this.TSDBServers.get(i);
    openTSDB = new OpenTSDB(address.getHostName(), address.getPort());
    openTSDB.connect();
  }

  @Override
  public void putMetrics(MetricsRecord record) {
    long timestamp = record.timestamp() / 1000L;
    String recordName = record.context() + "." + record.name();

    StringBuilder tagBuilder = new StringBuilder();
    for (MetricsTag tag : record.tags()) {
      tagBuilder.append(tag.name()).append("=").
              append(tag.value()).append(" ");
    }
    String tagValue = tagBuilder.toString();

    for (AbstractMetric metric : record.metrics()) {
      String metricName = recordName + "." + metric.name();
      String dataPoint = String.valueOf(metric.value());
      String entry = String.format(this.TSDBEntry, new Object[]{metricName,
              Long.valueOf(timestamp), dataPoint, tagValue});
      try {
        this.openTSDB.write(entry);
      } catch (IOException exception) {
        try {
          this.openTSDB.close();
        } catch (Exception e1) {
          throw new MetricsException("Error closing connection to OpenTSDB", e1);
        }
      }
    }

  }

  @Override
  public void flush() {
    try {
      openTSDB.flush();
    } catch (Exception e) {
      LOG.warn("Error flushing metrics to OpenTSDB", e);
      try {
        openTSDB.close();
      } catch (Exception e1) {
        throw new MetricsException("Error closing connection to OpenTSDB", e1);
      }
    }
  }

  @Override
  public void close() throws IOException {
    this.openTSDB.close();
  }

  public static class OpenTSDB {
    private final static int MAX_CONNECTION_FAILURES = 10;

    private String serverHost;
    private int serverPort;
    private Writer writer = null;
    private Socket socket = null;
    private int connectionFailures = 0;

    public OpenTSDB(String serverHost, int serverPort) {
      this.serverHost = serverHost;
      this.serverPort = serverPort;
    }

    public void connect() {
      if (isConnected()) {
        throw new MetricsException("Already connected to OpenTSDB");
      }
      if (tooManyConnectionFailures()) {
        return;
      }
      try {
        // Open a connection to OpenTSDB server.
        socket = new Socket(serverHost, serverPort);
        writer = new OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8);
      } catch (Exception e) {
        connectionFailures++;
        if (tooManyConnectionFailures()) {
          // first time when connection limit reached, report to logs
          LOG.error("Too many connection failures, would not try to connect again.");
        }
        throw new MetricsException("Error creating connection, "
                + serverHost + ":" + serverPort, e);
      }
    }

    public void write(String msg) throws IOException {
      if (!isConnected()) {
        connect();
      }
      if (isConnected()) {
        writer.write(msg);
      }
    }

    public void flush() throws IOException {
      if (isConnected()) {
        writer.flush();
      }
    }

    public boolean isConnected() {
      return socket != null && socket.isConnected() && !socket.isClosed();
    }

    public void close() throws IOException {
      try {
        IOUtils.closeStream(writer);
        IOUtils.closeSocket(socket);
      } finally {
        socket = null;
        writer = null;
      }
    }

    private boolean tooManyConnectionFailures() {
      return connectionFailures > MAX_CONNECTION_FAILURES;
    }

  }


}
