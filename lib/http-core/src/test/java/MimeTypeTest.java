import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.spotify.heroic.lib.httpcore.MimeType;
import org.junit.Test;

public class MimeTypeTest {
    @Test
    public void testSimple() {
        final MimeType mime = MimeType.parse("text/plain");
        assertEquals("text", mime.getType());
        assertEquals("plain", mime.getSubType());
    }

    @Test
    public void testMatches() {
        assertTrue(MimeType.WILDCARD.matches(MimeType.TEXT_PLAIN));
        assertTrue(MimeType.parse("text/*").matches(MimeType.TEXT_PLAIN));
        assertTrue(MimeType.parse("text/plain").matches(MimeType.TEXT_PLAIN));
        assertFalse(MimeType.parse("application/*").matches(MimeType.TEXT_PLAIN));
        assertFalse(MimeType.parse("application/json").matches(MimeType.TEXT_PLAIN));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegal() {
        MimeType.parse("*/plain");
    }
}
