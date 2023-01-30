package org.example.exceptions;

public class WrongDataException extends Exception {
    private final String line;

    public WrongDataException(String msg, String line) {
        super(msg);
        this.line = line;
    }

    public String getLine() {
        return line;
    }
}
