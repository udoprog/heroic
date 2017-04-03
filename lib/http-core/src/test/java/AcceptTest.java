import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.spotify.heroic.lib.httpcore.Accept;
import org.junit.Test;

public class AcceptTest {
    @Test
    public void testParse() {
        final Accept accept = Accept.parse("*/*, text/plain; q=0.8");
        System.out.println(accept);
    }

    @Test
    public void testWildcard() {
        assertTrue(Accept.ANY == Accept.parse("*/*"));
        assertTrue(Accept.ANY == Accept.parse("*/*; q=1.0"));
        // wildcards do are not identical when extensions are present
        assertFalse(Accept.ANY == Accept.parse("*/*; q=1.0; ext=value"));
    }
}
