package com.ebay.traffic.chocolate.flink.nrt.app;

import org.junit.Test;

import java.util.Map;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class UtpMonitorAppTest {
    @Test
    public void getDomainFromURL() {
        //test case 1: url is null or empty
        String urlquerystring = "";
        assertEquals(null, UtpMonitorApp.getDomainFromUrl(urlquerystring));

        urlquerystring = null;
        assertEquals(null, UtpMonitorApp.getDomainFromUrl(urlquerystring));

        //test case 2: url start with http/https
        urlquerystring = "https://www.ebay.com/itm/161579456719?_trkparms=pageci%3A0098e381-3006-11ed-b321-62ca2cece5a5%7Cparentrq%3A20de56651830a120d4a0913efffd7bef%7Ciid%3A1";
        assertEquals("www.ebay.com", UtpMonitorApp.getDomainFromUrl(urlquerystring));

        urlquerystring = "http://www.ebay.com/itm/161579456719?_trkparms=pageci%3A0098e381-3006-11ed-b321-62ca2cece5a5%7Cparentrq%3A20de56651830a120d4a0913efffd7bef%7Ciid%3A1";
        assertEquals("www.ebay.com", UtpMonitorApp.getDomainFromUrl(urlquerystring));

        //test case 3: url not start with http/https
        urlquerystring = "ebay://link?nav=item.view&id=255681281576&mkcid=27&osub=bcbde8e54dc6ca8386d721639b0ca2fd%257E1652783815494MC_T_PREB_APAC_EN_T&mkevt=1&crd=20220822153834&emsid=0&ch=osgood&segname=1652783815494MC_1652783815494MC_T_PREB_APAC_EN_T&bu=45618498603&mkpid=2";
        assertEquals(null, UtpMonitorApp.getDomainFromUrl(urlquerystring));
    }

    @Test
    public void getPageType(){
        String url = "";
        //test case 1: url is null or empty
        assertEquals(null, UtpMonitorApp.getPageType(url));
        url = null;
        assertEquals(null, UtpMonitorApp.getPageType(url));

        //test case 2: /i
        url = "https://www.ebay.com/i/265835912363?mkevt=1&mkpid=2&emsid=e90001.m43.l1123&mkcid=8&bu=43705637152&osub=620f3412e10dfecb82c6f5075b07386b%257ETE80101_T_AGM&segname=TE80101_T_AGM&crd=20220906080500&ch=osgood&trkId=0A0F1205-C59EA37B53D-0182CF421058-0000000001F09CDB&mesgId=3024&plmtId=700001&recoId=265835912363&recoPos=1";
        assertEquals("i", UtpMonitorApp.getPageType(url));

        //test case 3: /itm
        url = "https://www.ebay.com/itm/132528584269?var=431881057561&norover=1&mkevt=1&mkcid=4&mkrid=711-164965-939394-1&mpt=[CACHEBUSTER]&gdpr=$%7BGDPR%7D&gdpr_consent=$%7BGDPR_CONSENT_929%7D&campaignid=13051117164&gclid=EAIaIQobChMIgNTPhOWA-gIV8Yp_BB2XLQCMEAEYASAEEgJoDvD_BwE&ff18=mWeb&siteid=0&ipn=admain2&placement=561400&gclid=EAIaIQobChMIgNTPhOWA-gIV8Yp_BB2XLQCMEAEYASAEEgJoDvD_BwE";
        assertEquals("itm", UtpMonitorApp.getPageType(url));

        //test case 4: /sch
        url = "https://www.ebay.com/sch/i.html?_nkw=motor%20v6&norover=1&mkevt=1&mkrid=711-132282-41179-0&mkcid=2&keyword=motor%20v6&crlp=496120426281_&MT_ID=&geo_id=&rlsatarget=kwd-294816226536&adpos=&device=c&loc=1003316&poi=&abcId=&cmpgn=1897880283&sitelnk=&adgroupid=69383782294&network=g&matchtype=b&gclid=EAIaIQobChMIwPu5t4qs3AIVAQAAAB0BAAAAEAAYACAAEgJVzfD_BwE";
        assertEquals("sch", UtpMonitorApp.getPageType(url));

        //test case 5: /b
        url = "https://www.ebay.de/b/Re-Store/bn_7114979731?norover=1&mkevt=1&mkcid=4&mkrid=707-164978-451605-2&mpt=[CACHEBUSTER]&gdpr=${GDPR}&gdpr_consent=${GDPR_CONSENT_929}&campaignid=14653204992&gclid=Cj0KCQjw39uYBhCLARIsAD_SzMT071r528SZmqFPobQYToYlK4uloaeZqiOeXIrEzDgc5vkY5keGqSgaAh2yEALw_wcB&siteid=77&ipn=admain2&placement=561477&gclid=Cj0KCQjw39uYBhCLARIsAD_SzMT071r528SZmqFPobQYToYlK4uloaeZqiOeXIrEzDgc5vkY5keGqSgaAh2yEALw_wcB";
        assertEquals("b", UtpMonitorApp.getPageType(url));

        //test case 6: /e
        url = "https://www.ebay.com.au/e/_home-garden/c-top-baking-essentials?norover=1&mkevt=1&mkcid=4&mkrid=705-165088-612222-4&mpt=[CACHEBUSTER]&gdpr=0&gdpr_consent=&ff5=76927|415729&ff8=2138376&ff9=${PUBLISHERID}&ff18=mWeb&siteid=0&ipn=admain2&placement=562254";
        assertEquals("e", UtpMonitorApp.getPageType(url));

        //test case 7: /vod
        url = "https://www.ebay.com/vod/FetchOrderDetails?itemId=115380425913&transactionId=2316271650001&mkevt=1&mkpid=0&emsid=e11401.m44.l44718&mkcid=7&ch=osgood&euid=08bcc471a69d478f980b469087b8e2f1&bu=43207966083&osub=-1%7E1&crd=20220906093437&segname=11401";
        assertEquals("vod", UtpMonitorApp.getPageType(url));

        //test case 8: /ulk/message
        url = "https://www.ebay.com/ulk/messages/reply?M2MContact&item=175403355851&requested=cynq4-44&qid=2657320755011&redirect=0&self=vintage_and_mint&mkevt=1&mkpid=0&emsid=e11051.m44.l1139&mkcid=7&ch=osgood&euid=98360a68515a4a139da79c7a76a62d42&bu=43024922706&osub=-1~1&crd=20220905174843&segname=11051";
        assertEquals("ulk", UtpMonitorApp.getPageType(url));

        //test case 9:/ulk/usr
        url = "https://www.ebay.co.uk/ulk/usr/stockwellbrentford?mkevt=1&mkpid=0&emsid=e11051.m44.l1181&mkcid=7&ch=osgood&euid=dc6e5bcfdb314387aab04dc64b24ee6d&bu=43040615778&osub=-1%7E1&crd=20220906051219&segname=11051";
        assertEquals("ulk", UtpMonitorApp.getPageType(url));

        //test case 10: /ws
        url = "https://contact.ebay.co.uk/ws/eBayISAPI.dll?M2MContact&item=203642442923&requested=mainla-95&qid=2613934012018&redirect=0&mkevt=1&mkpid=0&emsid=e11050.m44.l1139&mkcid=26&ch=osgood&euid=1aee66d0a65c46d39230c10e96438748&bu=44807970017&osub=-1~1&crd=20220906110313&segname=11050";
        assertEquals("ws", UtpMonitorApp.getPageType(url));

        //test case 11: /p
        url = "https://www.ebay.com/p/905537090?norover=1&mkevt=1&mkrid=&mkcid=2&keyword=&crlp=-5799791041418980234_&MT_ID=&geo_id=&rlsatarget=&adpos=&device=t&mktype=&loc=&poi=&abcId=&cmpgn=&sitelnk=&adgroupid=&network=&matchtype=b";
        assertEquals("p", UtpMonitorApp.getPageType(url));

        //test case 12: homepage
        url = "https://www.ebay.com?var=431881057561&norover=1&mkevt=1&mkcid=4&mkrid=711-164965-939394-1&mpt=[CACHEBUSTER]&gdpr=$%7BGDPR%7D&gdpr_consent=$%7BGDPR_CONSENT_929%7D&campaignid=13051117164&gclid=EAIaIQobChMIgNTPhOWA-gIV8Yp_BB2XLQCMEAEYASAEEgJoDvD_BwE&ff18=mWeb&siteid=0&ipn=admain2&placement=561400&gclid=EAIaIQobChMIgNTPhOWA-gIV8Yp_BB2XLQCMEAEYASAEEgJoDvD_BwE";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.com.au?var=0&mkevt=1&mkcid=1&mkrid=705-53470-19255-0&campid=5338590836&toolid=10044";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.ca?mkevt=1&mkpid=0&emsid=e11021.m43.l1120&mkcid=7&ch=osgood&euid=b7b3e948c57442ad8532d23f2d5469f3&bu=43161315825&ut=RU&exe=0&ext=0&osub=-1%7E1&crd=20220906073114&segname=11021";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.de?mkevt=1&mkcid=1&mkrid=707-53477-19255-0&campid=5338364441&customid=254794074934_131090&toolid=11000&_trkparms=ispr%3D1&amdata=enc%3A1J38H4-6YS16sM-ymgPyarw87";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.at?campid=5337337894&customid&mkcid=1&mkevt=1&mkrid=5221-53469-19255-0&toolid=10018";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.ch?mkevt=1&mkcid=1&mkrid=5222-53480-19255-0&campid=5338376187&customid=354055539312_220&toolid=11000";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.es?mkevt=1&mkcid=1&mkrid=1185-53479-19255-0&campid=5338391703&customid=334342507738_131090&toolid=11000";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.fr?mkevt=1&mkcid=1&mkrid=709-53476-19255-0&campid=5338376269&toolid=11000";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.befr.ebay.be?mkevt=1&mkcid=1&mkrid=1553-53471-19255-0&campid=5338430858&customid=203596152111_888&toolid=11000";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.in?more=http%3A%2F%2Fwww.ebay.in&mkevt=1&mkcid=2&mkrid=4686-203594-43235-113&ufes_redirect=true";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.it/?mkevt=1&mkcid=1&mkrid=724-53478-19255-0&campid=5338491329&toolid=20006&customid=ec4827eb1d9e54bb&_trkparms=ispr%3D1&amdata=enc%3A1kFoARB9STQiocgQJOJ3czQ75";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.benl.ebay.be?mkevt=1&mkcid=1&mkrid=1553-53471-19255-0&campid=5338430852&customid=134022135207_619&toolid=11000";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.cafr.ebay.ca?mkevt=1&mkpid=0&emsid=e13024.m65708.l1332&mkcid=7&ch=osgood&euid=9e6bf9a8f7db48a6b8b1640948c7c804&bu=45309009838&osub=-1~1&crd=20220906072152&segname=13024";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.com.hk?mkevt=1&mkpid=0&emsid=e13024.m65708.l1332&mkcid=7&ch=osgood&euid=9e6bf9a8f7db48a6b8b1640948c7c804&bu=45309009838&osub=-1~1&crd=20220906072152&segname=13024";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.pl?mkevt=1&mkpid=0&emsid=e13024.m65708.l1332&mkcid=7&ch=osgood&euid=9e6bf9a8f7db48a6b8b1640948c7c804&bu=45309009838&osub=-1~1&crd=20220906072152&segname=13024";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.com.sg?mkevt=1&mkpid=0&emsid=e13024.m65708.l1332&mkcid=7&ch=osgood&euid=9e6bf9a8f7db48a6b8b1640948c7c804&bu=45309009838&osub=-1~1&crd=20220906072152&segname=13024";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.com.my?mkevt=1&mkpid=0&emsid=e13024.m65708.l1332&mkcid=7&ch=osgood&euid=9e6bf9a8f7db48a6b8b1640948c7c804&bu=45309009838&osub=-1~1&crd=20220906072152&segname=13024";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.ph?mkevt=1&mkpid=0&emsid=e13024.m65708.l1332&mkcid=7&ch=osgood&euid=9e6bf9a8f7db48a6b8b1640948c7c804&bu=45309009838&osub=-1~1&crd=20220906072152&segname=13024";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.nl?mkevt=1&mkpid=0&emsid=e13024.m65708.l1332&mkcid=7&ch=osgood&euid=9e6bf9a8f7db48a6b8b1640948c7c804&bu=45309009838&osub=-1~1&crd=20220906072152&segname=13024";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.co.uk?mkevt=1&mkpid=0&emsid=e13024.m65708.l1332&mkcid=7&ch=osgood&euid=9e6bf9a8f7db48a6b8b1640948c7c804&bu=45309009838&osub=-1~1&crd=20220906072152&segname=13024";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.ie?mkevt=1&mkpid=0&emsid=e13024.m65708.l1332&mkcid=7&ch=osgood&euid=9e6bf9a8f7db48a6b8b1640948c7c804&bu=45309009838&osub=-1~1&crd=20220906072152&segname=13024";
        assertEquals("homepage", UtpMonitorApp.getPageType(url));

        url = "https://www.ebay.ie/abc?mkevt=1&mkpid=0&emsid=e13024.m65708.l1332&mkcid=7&ch=osgood&euid=9e6bf9a8f7db48a6b8b1640948c7c804&bu=45309009838&osub=-1~1&crd=20220906072152&segname=13024";
        assertEquals("others", UtpMonitorApp.getPageType(url));

        //test case 13: others
        url = "https://pages.ebay.ie?mkevt=1&mkpid=0&emsid=e13024.m65708.l1332&mkcid=7&ch=osgood&euid=9e6bf9a8f7db48a6b8b1640948c7c804&bu=45309009838&osub=-1~1&crd=20220906072152&segname=13024";
        assertEquals("others", UtpMonitorApp.getPageType(url));

        //test case 14: /cnt
        url = "https://www.ebay.co.uk/cnt/ReplyToMessages?M2MContact&item=314030905614&requested=malananay&qid=2630219706010&redirect=0&self=worn.again&mkevt=1&mkpid=0&emsid=e11051.m44.l1139&mkcid=26&ch=osgood&euid=b256cbdc62504ea4909acc74524e6086&bu=44404117283&osub=-1%7E1&crd=20220906075737&segname=11051&ul_noapp=true";
        assertEquals("cnt", UtpMonitorApp.getPageType(url));

        //test case 15: /sl/
        url = "https://www.ebay.de/sl/sell?norover=1&mkevt=1&mkcid=4&mkrid=707-166238-447818-8&mpt=[CACHEBUSTER]&gdpr=$%7BGDPR%7D&gdpr_consent=$%7BGDPR_CONSENT_929%7D&campaignid=18247229738&gclid=&siteid=77&ipn=admain2&placement=570740&wbraid=Ck8KCAjwvNaYBhBeEj8AOHZiB8YUdMQ13wMVhHvEfGa4rk7YpNpm7XA92Ok9UbEHso0bpvh2bNt08S87wjy5DHb_vBPaWoL1a8xitiAaAq3y";
        assertEquals("sl", UtpMonitorApp.getPageType(url));

        //test case 16: /signin/
        url = "https://www.ebay.com/signin/g/v%5E1.1%23i%5E1%23r%5E1%23I%5E3%23f%5E0%23p%5E3%23t%5EUl4xMF8xMDo1Rjk4NkNFMDEwRkJFRkI2M0VFNEZCMDg3NkIzMjYzMV8yXzEjRV4yNjA%3D?mkevt=1&mkpid=0&emsid=e11400.m44.l5154&mkcid=7&ch=osgood&euid=fe5915d9954d43be8e21f641c8c2a513&bu=45626214536&exe=0&ext=0&osub=-1%7E1&crd=20220902104835&segname=11400";
        assertEquals("signin", UtpMonitorApp.getPageType(url));

        //test case 17: /fdbk/
        url = "https://www.ebay.com/fdbk/feedback_profile/rondel_7630?mkevt=1&mkpid=0&emsid=e11051.m44.l1183&mkcid=26&ch=osgood&euid=d36f6b33add14adbb6fb69bf175f8551&bu=43745543217&osub=-1%7E1&crd=20220905202845&segname=11051";
        assertEquals("fdbk", UtpMonitorApp.getPageType(url));

        //test case 18: /rtn/
        url = "https://www.ebay.co.uk/rtn/Return/ReturnsDetail?returnId=5219880393&mkevt=1&mkpid=0&emsid=e11923.m4972.l9248&mkcid=7&ch=osgood&euid=9d47a2f63f9d46c296ab4bc003e29cdc&bu=44164445702&exe=euid&ext=39554&osub=-1%7E1&crd=20220906081630&segname=11923";
        assertEquals("rtn", UtpMonitorApp.getPageType(url));


    }

    @Test
    public void getUFESSignal(){
        Map<String, String> payload = new HashMap<String, String>();
        //test case 1: no isUfes tag
        payload.put("description", "no isUfes tag");
        assertEquals("NULL", UtpMonitorApp.getUFESSignal(payload));

        //test case 2: isUfes = true
        payload.put("isUfes", "true");
        assertEquals("true", UtpMonitorApp.getUFESSignal(payload));

        //test case 3: isUfes = false
        payload.put("isUfes", "false");
        assertEquals("false", UtpMonitorApp.getUFESSignal(payload));

    }

    @Test
    public void testHomepagePattern(){

        String domain = "www.ebay.com";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "ww1.ebay.com";
        assertEquals(false, UtpMonitorApp.isHomepageDomain(domain));

        domain = "ebay.com";
        assertEquals(false, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.com";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.befr.ebay.be";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.benl.ebay.be";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.cafr.ebay.ca";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.caf.ebay.ca";
        assertEquals(false, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.ca";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.de";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.at";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.ch";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.es";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.fr";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.in";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.it";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.pl";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.ph";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.nl";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.ie";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.com.au";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.com.hk";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.com.sg";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.com.my";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.co.uk";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.co.uk1";
        assertEquals(false, UtpMonitorApp.isHomepageDomain(domain));


        domain = "WWW.EBAY.co.uk";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "www.ebay.at.uk";
        assertEquals(false, UtpMonitorApp.isHomepageDomain(domain));


        domain = "m.befr.ebay.be";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.benl.ebay.be";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.cafr.ebay.ca";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.caf.ebay.ca";
        assertEquals(false, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.ca";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.de";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.at";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.ch";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.es";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.fr";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.in";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.it";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.pl";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.ph";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.nl";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.ie";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.com.au";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.com.hk";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.com.sg";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.com.my";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.co.uk";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.co.uk1";
        assertEquals(false, UtpMonitorApp.isHomepageDomain(domain));


        domain = "M.EBAY.co.uk";
        assertEquals(true, UtpMonitorApp.isHomepageDomain(domain));

        domain = "m.ebay.at.uk";
        assertEquals(false, UtpMonitorApp.isHomepageDomain(domain));


    }

}
