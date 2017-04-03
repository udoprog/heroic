package com.spotify.heroic.lib.httpcore;

public enum Family {
    INFORMATIONAL, SUCCESSFUL, REDIRECTION, CLIENT_ERROR, SERVER_ERROR, OTHER;

    public static Family familyOf(final int statusCode) {
        switch (statusCode / 100) {
            case 1:
                return INFORMATIONAL;
            case 2:
                return SUCCESSFUL;
            case 3:
                return REDIRECTION;
            case 4:
                return CLIENT_ERROR;
            case 5:
                return SERVER_ERROR;
            default:
                return OTHER;
        }
    }
}
