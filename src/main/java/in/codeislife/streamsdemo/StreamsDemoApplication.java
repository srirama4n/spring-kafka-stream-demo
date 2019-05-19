package in.codeislife.streamsdemo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import in.codeislife.streamsdemo.binding.ProvisionBinding;
import in.codeislife.streamsdemo.model.PrevisionCertificateDetails;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableBinding(ProvisionBinding.class)
public class StreamsDemoApplication {

    public static final ObjectMapper mapper = new ObjectMapper();

    @Component
    public class EventSource implements ApplicationRunner {

        private MessageChannel messageChannel;
        private Log log = LogFactory.getLog(getClass());

        public EventSource(ProvisionBinding binding) {
            this.messageChannel = binding.output();
        }

        @Override
        public void run(ApplicationArguments args) throws Exception {
            List<String> subjects = Arrays.asList("heelo", "test", "Myname", "cert",  "wells");

            Runnable runnable = () -> {

                String rSubject = subjects.get(new Random().nextInt(subjects.size()));

                PrevisionCertificateDetails previsionCertificateDetails =
                        new PrevisionCertificateDetails("1", UUID.randomUUID().toString(), rSubject);

                Message<?> message = null;
                try {
                    message = MessageBuilder
                            .withPayload(mapper.writeValueAsString(previsionCertificateDetails).getBytes())
                            .setHeader(KafkaHeaders.MESSAGE_KEY, previsionCertificateDetails.getGuid().getBytes())
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





    public static void main(String[] args) {
        SpringApplication.run(StreamsDemoApplication.class, args);
    }

}


