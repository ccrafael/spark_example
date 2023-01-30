package org.example.mapper;


import lombok.extern.log4j.Log4j2;
import org.apache.spark.api.java.function.Function;
import org.example.exceptions.WrongDataException;
import org.example.model.Input;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Log4j2
public class InputMapper implements Function<String, Input> {
    private static final Pattern filePattern = Pattern.compile("(input)-([0-9]+)(\\.csv)");

    @Override
    public Input call(String line) throws Exception {

        try {
            String[] columns = line.split(" *, *");
            if (columns.length != 4) {
                throw new WrongDataException("Incorrect line.", line);
            }
            return Input.builder()
                    .filePartition(decodePartition(columns[0]))
                    .number(Integer.parseInt(columns[1]))
                    .company(columns[2])
                    .value(Integer.parseInt(columns[3]))
                    .build();
        } catch (IllegalArgumentException e) {
            throw new WrongDataException("Incorrect line.", line);
        }
    }

    private Integer decodePartition(String fileName) {
        Matcher matcher = filePattern.matcher(fileName);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(2));
        } else {
            throw new IllegalArgumentException("Incorrect file name");
        }
    }

}
