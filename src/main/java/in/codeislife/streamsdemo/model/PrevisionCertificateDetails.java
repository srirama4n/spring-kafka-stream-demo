package in.codeislife.streamsdemo.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrevisionCertificateDetails implements Serializable {
    private String id;
    private String guid;
    private String subject;
}
