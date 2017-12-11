import com.google.common.io.Files;
import com.google.common.io.MoreFiles;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.activemq.usage.SystemUsage;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import javax.jms.*;
import java.io.File;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.testng.Assert.assertNotNull;

public class ReconnectionTest {

  private static final String baseUri = "tcp://localhost:60606?persistence=true";

  private static final String connectionUri = "failover:(tcp://localhost:60606)"
      + "?initialReconnectDelay=100"
      + "&maxReconnectAttempts=100"
      + "&useExponentialBackOff=false"
      + "&maxReconnectDelay=10"
      + "&trackMessages=true"
      + "&warnAfterReconnectAttempts=10";

  private BrokerService broker;

  private File dataDir;

  private PooledConnectionFactory factory;

  private Connection consumerConnection;

  private Connection producerConnection;


  @BeforeMethod
  public void beforeMethod() throws Exception {
    dataDir = Files.createTempDir();
    startBroker();
    factory = new PooledConnectionFactory(connectionUri);
    factory.setExpiryTimeout(1000);
    factory.setReconnectOnException(true);
    factory.start();
    consumerConnection = factory.createConnection();
    consumerConnection.start();
    producerConnection = factory.createConnection();
    producerConnection.start();
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    closeQuietly(producerConnection);
    closeQuietly(consumerConnection);
    factory.stop();
    stopBroker();
    MoreFiles.deleteRecursively(dataDir.toPath());
  }

  @Test
  public void testPublishAndReceive() throws JMSException {
    String queue = "foo";
    MessageConsumer consumer = createConsumer(queue);
    TextMessage published = publish(queue, "message 1");
    TextMessage received = TextMessage.class.cast(consumer.receive(1000));

    assertNotNull(received);
    assertThat(received.getText(), equalTo(published.getText()));
  }

  @Test
  public void testPublishAndRecieveAfterDisconnect() throws Exception {
    String queue = "foo";
    MessageConsumer consumer = createConsumer(queue);
    TextMessage published1 = publish(queue, "message 1");
    TextMessage received1 = TextMessage.class.cast(consumer.receive(1000));

    broker.stop();
    broker.start();

    await().atMost(10, TimeUnit.SECONDS).until(() -> broker.isStarted());

    TextMessage published2 = publish(queue, "message 2");
    TextMessage received2 = TextMessage.class.cast(consumer.receive(1000));

    assertNotNull(received1);
    assertThat(received1.getText(), equalTo(published1.getText()));

    assertNotNull(received2);
    assertThat(received1.getText(), equalTo(published2.getText()));
  }

  private TextMessage publish(String queue, String message) throws JMSException {
    Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    try {
      TextMessage msg = session.createTextMessage(message);
      MessageProducer producer = session.createProducer(session.createQueue(queue));
      producer.send(msg);
      return msg;
    } finally {
      closeQuietly(session);
    }
  }

  private MessageConsumer createConsumer(String queue) throws JMSException {
    Session session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    return session.createConsumer(session.createQueue(queue));
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

}
