package in.codeislife.streamsdemo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

        private Log log = LogFactory.getLog(getClass());

        @StreamListener
        public void process(
                @Input(AnalyticsBinding.PAGE_VIEWS_IN) KStream<String, String> events) {
            KTable<String, String> kTable = events
                    .groupByKey()
                    .reduce((aggValue, currValue) -> currValue, Materialized.as(AnalyticsBinding.PAGE_COUNT_MV));

            kTable.queryableStoreName();
        }
    }


    @RestController
    public static class TestController {

        private Log log = LogFactory.getLog(getClass());

        private final InteractiveQueryService interactiveQueryService;

        public TestController(InteractiveQueryService interactiveQueryService) {
            this.interactiveQueryService = interactiveQueryService;
        }

        @GetMapping("/count")
        public void process() {
            ReadOnlyKeyValueStore<String, String> store = interactiveQueryService
                    .getQueryableStore(AnalyticsBinding.PAGE_COUNT_MV, QueryableStoreTypes.<String, String>keyValueStore());
            HostInfo hostInfo = interactiveQueryService.getHostInfo(AnalyticsBinding.PAGE_COUNT_MV, "MMyname", new StringSerializer());
            log.info("Host "+hostInfo.host()+" : "+hostInfo.port());
            KeyValueIterator<String, String> all = store.all();
            while (all.hasNext()) {

                log.info("Next value is : " + String.valueOf(all.next()));
            }

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

