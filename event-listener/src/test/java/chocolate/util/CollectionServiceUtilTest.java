package chocolate.util;

import com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CollectionServiceUtilTest {
    @Test
    public void testIsDuplicateItmClick() {
        boolean isDuplicateItemClick = false;

        // non-itm page
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/i/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.co.uk/scp/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        // item page with title
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/asdfwerw/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/23362adfawerqwet/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/adfawerqwe321434/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        // bot click
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "GingerClient"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", true, true, true);
        assertEquals(false, isDuplicateItemClick);

        // user clicks from special sites
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.nl/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://befr.ebay.be/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com.my/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        // user clicks from dweb
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, false, false);
        assertEquals(false, isDuplicateItemClick);

        // user clicks from tablet
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, true, false);
        assertEquals(false, isDuplicateItemClick);

        // user clicks from native app
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, false, false);
        assertEquals(false, isDuplicateItemClick);

        // user clicks from non-special sites and mobile phone web
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(true, isDuplicateItemClick);

        // other marketing status code
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("200", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("404", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        // throw exception
        isDuplicateItemClick = false;
        try {
            isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                    , null, false, true, true);
        } catch (Exception e) {
            System.out.println(e);
        }
        assertEquals(false, isDuplicateItemClick);

        try {
            isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", null
                    , "https://www.ebay.com/itm/233622232591?mkevt=1", false, true, true);
        } catch (Exception e) {
            System.out.println(e);
        }
        assertEquals(false, isDuplicateItemClick);

    }
}
