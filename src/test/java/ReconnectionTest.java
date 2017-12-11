import com.google.common.io.Files;
import com.google.common.io.MoreFiles;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.activemq.usage.SystemUsage;
import org.testng.annotations.*;

import javax.annotation.Nullable;
import javax.jms.*;
import java.io.File;
import java.net.URI;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.testng.Assert.assertNotNull;

public class ReconnectionTest {

  private String baseUri;

  private String connectionUri;

  private BrokerService broker;

  private File dataDir;

  private PooledConnectionFactory factory;

  @BeforeClass
  public void createBroker() throws Exception {
    dataDir = Files.createTempDir();
    baseUri = "tcp://localhost:60606?persistence=true";
    connectionUri = "failover:(tcp://localhost:60606)"
        + "?initialReconnectDelay=10"
        + "&maxReconnectAttempts=200000"
        + "&useExponentialBackOff=false"
        + "&maxReconnectDelay=10"
        + "&trackMessages=true"
        + "&warnAfterReconnectAttempts=10";
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    startBroker();
    factory = new PooledConnectionFactory(connectionUri);
    //factory.start();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    factory.stop();
    stopBroker();
  }

  private void startBroker() throws Exception {
    checkState(broker == null || broker.isStopped());

    broker = new BrokerService();

    // Set limits to avoid warnings since I don't have enough resources
    // to cover the default values.
    SystemUsage systemUsage = broker.getSystemUsage();
    systemUsage.getMemoryUsage().setLimit(500000000);
    systemUsage.getTempUsage().setLimit(500000000);

    TransportConnector connector = new TransportConnector();
    connector.setUri(new URI(baseUri));
    broker.addConnector(connector);
    broker.setDataDirectory(dataDir.getCanonicalPath());
    broker.start();
  }

  private void stopBroker() throws Exception {
    if (broker != null) {
      broker.stop();
      broker = null;
    }
  }

  @AfterClass
  public void afterClass() throws Exception {
    MoreFiles.deleteRecursively(dataDir.toPath(), ALLOW_INSECURE);
  }

  @Test
  public void testSubscribeAndPublish() throws JMSException {
    String queue = "foo";
    MessageConsumer consumer = createConsumer(factory, queue);
    TextMessage published = publish(factory, queue, "message 1");
    TextMessage received = TextMessage.class.cast(consumer.receive(10000));
    assertNotNull(received);
    assertThat(received.getText(), equalTo(published.getText()));
  }

  private TextMessage publish(ConnectionFactory factory, String queue, String message) throws JMSException {
    Connection connection = factory.createConnection();
    connection.start();
    Session session = null;
    try {
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      session.recover();
      TextMessage msg = session.createTextMessage(message);
      MessageProducer producer = session.createProducer(session.createQueue(queue));
      producer.send(msg);
      return msg;
    } finally {
      closeQuietly(session);
      closeQuietly(connection);
    }
  }

  private MessageConsumer createConsumer(ConnectionFactory factory, String queue) throws JMSException {
    Connection connection = factory.createConnection();
    connection.start();
    Session session = null;
    try {
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      return session.createConsumer(session.createQueue(queue));
    } catch (JMSException | RuntimeException e) {
      closeQuietly(connection);
      closeQuietly(session);
      closeQuietly(connection);
      throw e;
    }
  }

  private void stopQuietly(Connection connection) {
    try {
      if (connection != null) {
        connection.stop();
        connection.close();
      }
    } catch (JMSException e) {
      System.out.println("Error closing connection: " + e.getMessage());
    }
  }

  private void closeQuietly(@Nullable Session session) {
    try {
      if (session != null) {
        session.close();
      }
    } catch (JMSException e) {
      System.out.println("Error closing session: " + e.getMessage());
    }
  }

  private void closeQuietly(@Nullable Connection connection) {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (JMSException e) {
      System.out.println("Error closing connection: " + e.getMessage());
    }
  }

}
