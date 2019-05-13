
package in.codeislife.streamsdemo.processor;

import in.codeislife.streamsdemo.binding.AnalyticsBinding;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PageViewEventSink {


    @StreamListener
    public void process(
            @Input(AnalyticsBinding.PAGE_VIEWS_IN) KStream<String, String> events) {
        KTable<String, String> kTable = events
                .groupByKey()
                .reduce((aggValue, currValue) -> currValue, Materialized.as(AnalyticsBinding.PAGE_COUNT_MV));

        kTable.queryableStoreName();
    }
}
