package in.codeislife.streamsdemo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PageViewEvent implements Serializable {
    private String userId, page;
    private long duration;
}
