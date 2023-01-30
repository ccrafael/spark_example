package org.example.model;

import lombok.Builder;
import lombok.Data;
import lombok.With;

import java.io.Serializable;

@Data
@With
@Builder
public class Output implements Serializable {
    Integer number;
    String company;
    Integer value;
    Integer prevNumber;
    Integer prevValue;
    Integer partitionFile;

    @Override
    public String toString() {
        return String.format("%s,%s,%s,%s,%s", number, company, value, prevNumber, prevValue);
    }
}
