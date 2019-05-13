package in.codeislife.streamsdemo.binding;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface AnalyticsBinding {

    String PAGE_VIEWS_OUT = "pvout";
    String PAGE_VIEWS_IN = "pvin";
    String PAGE_COUNT_MV = "pcmv";

    @Input(PAGE_VIEWS_IN)
    KStream<?, ?> pageViewInt();

    @Output(PAGE_VIEWS_OUT)
    MessageChannel output();
}
