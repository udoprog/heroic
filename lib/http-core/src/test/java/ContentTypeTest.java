import com.spotify.heroic.lib.httpcore.ContentType;
import org.junit.Test;

public class ContentTypeTest {
    @Test
    public void parseTest() {
        final ContentType contentType = ContentType.parse("text/plain; charset=utf-8");

        System.out.println(contentType);
    }
}
