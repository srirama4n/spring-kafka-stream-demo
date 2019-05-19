package in.codeislife.streamsdemo.controller;

import in.codeislife.streamsdemo.binding.ProvisionBinding;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@Slf4j
public class TestController {

    private final InteractiveQueryService interactiveQueryService;

    public TestController(InteractiveQueryService interactiveQueryService) {
        this.interactiveQueryService = interactiveQueryService;
    }

    @GetMapping("/search")
    public List<String> process() {
        ReadOnlyKeyValueStore<String, String> store = interactiveQueryService
                .getQueryableStore(ProvisionBinding.PROVISION_MV, QueryableStoreTypes.<String, String>keyValueStore());
//        HostInfo hostInfo = interactiveQueryService.getHostInfo(ProvisionBinding.PROVISION_MV, "Myname", new StringSerializer());
//        log.info("Host " + hostInfo.host() + " : " + hostInfo.port());
        KeyValueIterator<String, String> data = store.all();
        List<String> response = new ArrayList<>();
        while (data.hasNext()) {
            KeyValue<String, String> keyValue = data.next();
            response.add(String.valueOf(keyValue));
        }
        return response;

    }

}