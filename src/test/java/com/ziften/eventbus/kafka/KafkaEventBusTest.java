package com.ziften.eventbus.kafka;

import com.google.common.eventbus.Subscribe;
import org.testng.annotations.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


public class KafkaEventBusTest {

    @Test
    public void testPostAndReceiveEvent() throws Exception {
        KafkaEventBus bus = new KafkaEventBus("127.0.0.1:2181", "unitTest");
        bus.start();

        MyListener listener = new MyListener();
        bus.register(listener);

        bus.post(new MyEvent(123, "Test event"));
        Thread.sleep(5000);
        bus.stop();

        assertThat(listener.getLastProcessedEvent(), instanceOf(MyEvent.class));
        assertThat(listener.getLastProcessedEvent().eventId, equalTo(123));

    }


     private class MyListener {

         private MyEvent lastProcessedEvent;

         @Subscribe
         private void processEvent(MyEvent event) {
             lastProcessedEvent = event;
         }

         public MyEvent getLastProcessedEvent() {
             return lastProcessedEvent;
         }
     }

    private class MyEvent {
        int eventId;
        String message;

        public MyEvent(int eventId, String message) {
            this.eventId = eventId;
            this.message = message;
        }

        public int getEventId() {
            return eventId;
        }

        public String getMessage() {
            return message;
        }
    }
}