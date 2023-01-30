package org.example.model;


import lombok.Builder;
import lombok.Data;
import lombok.With;

import java.io.Serializable;

@Data
@With
@Builder
public class Input implements Serializable {
    Integer filePartition;
    Integer number;
    String company;
    Integer value;
}
