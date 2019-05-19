
package in.codeislife.streamsdemo.processor;

import in.codeislife.streamsdemo.binding.ProvisionBinding;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class EventSink {


    @StreamListener
    public void process(
            @Input(ProvisionBinding.PROVISION_IN) KStream<String, String> events) {

        KTable<String, String> kTable = events
                .groupByKey()
                .reduce((aggValue, currValue) -> currValue, Materialized.as(ProvisionBinding.PROVISION_MV));

        kTable.queryableStoreName();
    }
}
