package com.spotify.heroic.lib.httpcore;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class MimeType {
    private static final String STAR = "*";

    public static final MimeType APPLICATION_JSON = parse("application/json");
    public static final MimeType TEXT_PLAIN = parse("text/plain");
    public static final MimeType TEXT_XML = parse("text/xml");
    public static final MimeType WILDCARD = new MimeType(STAR, STAR);

    private final String type;
    private final String subType;

    public static MimeType parse(final String mimeType) {
        final int slash = mimeType.indexOf('/');

        if (slash < 0) {
            throw new IllegalArgumentException(mimeType);
        }

        final String type = mimeType.substring(0, slash).trim();
        final String subType = mimeType.substring(slash + 1).trim();

        if (STAR.equals(type) && STAR.equals(subType)) {
            return WILDCARD;
        }

        if (type.equals(STAR) && !subType.equals(STAR)) {
            throw new IllegalArgumentException(mimeType);
        }

        return new MimeType(type, subType);
    }

    public boolean matches(final MimeType other) {
        if (other == WILDCARD) {
            return true;
        }

        if (!STAR.equals(type) && !STAR.equals(other.type) && !type.equals(other.type)) {
            return false;
        }

        if (!STAR.equals(subType) && !STAR.equals(other.subType) &&
            !subType.equals(other.subType)) {
            return false;
        }

        return true;
    }

    public boolean isWildcard() {
        if (this == WILDCARD) {
            return true;
        }

        return STAR.equals(type) && STAR.equals(subType);
    }

    @Override
    public String toString() {
        return type + "/" + subType;
    }
}
