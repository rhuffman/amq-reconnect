import com.beust.jcommander.internal.Lists;
import com.google.common.base.Preconditions;
import com.jayway.awaitility.Awaitility;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Message;
import javax.jms.MessageListener;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class TestingMessageListener implements MessageListener {

  private static final Logger logger = LoggerFactory.getLogger(TestingMessageListener.class);

  private final List<Message> messages = Collections.synchronizedList(Lists.newArrayList());
  private final AtomicInteger acknowledgedMessageCount = new AtomicInteger();

  @Override
  public void onMessage(Message message) {
    logger.debug("Handling message");
    ActiveMQMessage amqMessage = (ActiveMQMessage) message;

    // In ActiveMQ, messages have a callback that is invoked when they are
    // acknowledged by the consumer. In order to count how many messages
    // are acknowledged we have to replace that callback with one that will
    // increment our acknowledgement counter then invoke the original callback.
    Callback originalCallback = amqMessage.getAcknowledgeCallback();
    amqMessage.setAcknowledgeCallback(() -> {
      acknowledgedMessageCount.incrementAndGet();
      originalCallback.execute();
    });

    // The waitForMessageCount method waits for the size of the message list to
    // reach a particular value. Therefore, adding the message to the list of
    // is the last thing we do
    messages.add(message);
  }

  int getMessageCount() {
    return messages.size();
  }

  void waitForMessageCount(int count, long time, TimeUnit timeUnit) {
    Awaitility.await().atMost(time, timeUnit).until(() -> getMessageCount() == count);
  }

  List<Message> getMessages() {
    return Collections.unmodifiableList(messages);
  }

  Message getOnlyMessage() {
    Preconditions.checkState(messages.size() == 1, "Received %s messages", messages.size());
    return messages.get(0);
  }

  public int getAcknowledgedMessageCount() {
    return acknowledgedMessageCount.get();
  }

}
