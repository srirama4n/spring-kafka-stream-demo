package in.codeislife.streamsdemo.controller;

import in.codeislife.streamsdemo.binding.AnalyticsBinding;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringSerializer;
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
                .getQueryableStore(AnalyticsBinding.PAGE_COUNT_MV, QueryableStoreTypes.<String, String>keyValueStore());
        HostInfo hostInfo = interactiveQueryService.getHostInfo(AnalyticsBinding.PAGE_COUNT_MV, "Myname", new StringSerializer());
        log.info("Host " + hostInfo.host() + " : " + hostInfo.port());
        KeyValueIterator<String, String> all = store.all();
        List<String> strs = new ArrayList<>();
        while (all.hasNext()) {
            log.info("Next value is : " + String.valueOf(all.next()));
            strs.add(String.valueOf(all.next()));
        }
        return strs;

    }

}