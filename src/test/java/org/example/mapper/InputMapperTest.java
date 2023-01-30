package org.example.mapper;

import org.example.exceptions.WrongDataException;
import org.example.model.Input;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Tuple2;

import static org.example.mapper.InputMapperTest.Fixture.input;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class InputMapperTest {
    private InputMapper mapper;

    @BeforeEach
    public void setup() {
        mapper = new InputMapper();
    }

    @ParameterizedTest
    @ValueSource(strings = {"input-1.csv, 1, foo, 1", "input-1.csv , 1,foo,1", " input-1.csv , 1 , foo ,  1"})
    public void given_a_correct_line_when_call_then_map_to_input_object(String line) throws Exception {
        assertEquals(mapper.call(line), input);
    }

    @ParameterizedTest
    @ValueSource(strings = {"input-1.csv,a,foo,b", "input-1.csv,1,foo,b", "", " 1", "1,foo",
            "input-csv,2,3,4,6",
            "foo,1,var,2"})
    public void given_an_incorrect_line_when_call_then_throw_exception(String line) {
        assertThrows(WrongDataException.class, () -> mapper.call(line));
    }

    public interface Fixture {
        Input input = Input.builder()
                .filePartition(1)
                .number(1)
                .company("foo")
                .value(1)
                .build();
    }
}