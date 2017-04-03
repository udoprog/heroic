package com.spotify.heroic.server;

import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class Header {
    private static final Pattern NAME_PATTERN = Pattern.compile("^[a-z]+(-[a-z]+)*$");

    private final CharSequence name;
    private final CharSequence value;

    public static Header of(final CharSequence name, final CharSequence value) {
        validateName(name);
        return new Header(name, value);
    }

    public static void validateName(final CharSequence name) {
        if (!NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException(
                "invalid header name (must be all-lowercase): " + name);
        }
    }

    public static boolean nameEquals(final CharSequence a, final CharSequence b) {
        if (a.length() != b.length()) {
            return false;
        }

        final int len = a.length();

        for (int i = 0; i < len; i++) {
            final char l = Character.toLowerCase(a.charAt(i));
            final char r = Character.toLowerCase(b.charAt(i));

            if (l != r) {
                return false;
            }
        }

        return true;
    }
}
