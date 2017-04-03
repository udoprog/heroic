package com.spotify.heroic.lib.httpcore;

import java.util.regex.Pattern;
import lombok.Data;

@Data
public class NameValuePair {
    private static final Pattern EQUALS = Pattern.compile("=");

    private final String name;
    private final String value;

    public static NameValuePair parse(final String input) {
        final String[] nameValue = EQUALS.split(input);

        if (nameValue.length != 2) {
            throw new IllegalArgumentException(input);
        }

        final String name = nameValue[0].trim();
        final String value = nameValue[1].trim();

        return new NameValuePair(name, value);
    }
}
