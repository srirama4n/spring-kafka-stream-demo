package in.codeislife.streamsdemo.binding;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

public interface ProvisionBinding {

    String PROVISION_OUT = "pout";
    String PROVISION_IN = "pin";
    String PROVISION_MV = "pmv";

    @Input(PROVISION_IN)
    KStream<?, ?> provisonInt();

    @Output(PROVISION_OUT)
    MessageChannel output();
}
