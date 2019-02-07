package in.codeislife.streamsdemo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;

import javax.validation.Payload;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@SpringBootApplication
@EnableBinding(AnalyticsBinding.class)
public class StreamsDemoApplication {

    public static final ObjectMapper mapper = new ObjectMapper();

    @Component
    public static class PageViewEventSource implements ApplicationRunner {

        private MessageChannel messageChannel;
        private Log log = LogFactory.getLog(getClass());

        public PageViewEventSource(AnalyticsBinding binding) {
            this.messageChannel = binding.output();
        }

        @Override
        public void run(ApplicationArguments args) throws Exception {
            List<String> names = Arrays.asList("heelo", "Message1", "MMyname", "madhu", "wells");
            List<String> pages = Arrays.asList("initilizer", "sitemap", "bog", "news", "about");

            Runnable runnable = () -> {

                String rPage = pages.get(new Random().nextInt(pages.size()));
                String rName = names.get(new Random().nextInt(names.size()));

                PageViewEvent pageViewEvent = new PageViewEvent(rName, rPage, Math.random() > .5 ? 10 : 1000);

                Message<?> message = null;
                try {
                    message = MessageBuilder
                            .withPayload(mapper.writeValueAsString(pageViewEvent).getBytes())
                            .setHeader(KafkaHeaders.MESSAGE_KEY, pageViewEvent.getUserId().getBytes())
                            .build();
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }

                try {
                    log.info("sending " + message.toString());
                    this.messageChannel.send(message);
                    log.info("send " + message.toString());
                } catch (Exception e) {
                    log.error("Exception : " + e);
                }

            };

            Executors.newScheduledThreadPool(1)
                    .scheduleWithFixedDelay(runnable, 1, 1, TimeUnit.SECONDS);
        }
    }

    @Component
    public static class PageViewEventSink {

        Log log = LogFactory.getLog(getClass());

        @StreamListener
        public void process(
                @Input(AnalyticsBinding.PAGE_VIEWS_IN) KStream<String, String> events) {
            events.foreach((key, value) -> {
                try {
                    PageViewEvent o = mapper.readValue(value, PageViewEvent.class);
                    log.info(o.toString());
                    log.info("Received Message : "+ key.toString() + " : value "+o.getUserId());
                } catch (Exception e) {
                    e.printStackTrace();
                }

            });
            /*KTable<String, Long> count = events
                    .filter((s, value) -> value.getDuration() > 10)
                    .map((s, value) -> new KeyValue<>(value.getPage(), "0"))
                    .groupByKey()
                    .count(Materialized.as(AnalyticsBinding.PAGE_COUNT_MV));*/

        }
    }

    public static void main(String[] args) {
        SpringApplication.run(StreamsDemoApplication.class, args);
    }

}

interface AnalyticsBinding {

    String PAGE_VIEWS_OUT = "pvout";
    String PAGE_VIEWS_IN = "pvin";
    String PAGE_COUNT_MV = "pcmv";

    @Input(PAGE_VIEWS_IN)
    KStream<?, ?> pageViewInt();

    @Output(PAGE_VIEWS_OUT)
    MessageChannel output();
}


@Data
@AllArgsConstructor
@NoArgsConstructor
class PageViewEvent implements Serializable {
    private String userId, page;
    private long duration;
}

