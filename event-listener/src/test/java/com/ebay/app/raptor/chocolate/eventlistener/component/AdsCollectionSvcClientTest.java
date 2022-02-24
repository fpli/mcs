package com.ebay.app.raptor.chocolate.eventlistener.component;

import static org.junit.Assert.*;

import com.ebay.raptor.auth.RaptorSecureContext;
import com.ebay.raptor.auth.RaptorSecureContextProvider;
import org.junit.Test;

import static org.mockito.Mockito.*;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AdsCollectionSvcClientTest {
    AdsCollectionSvcClient adsCollectionSvcClient = new AdsCollectionSvcClient();

    @Test
    public void testAddUserIdToRequestHeader_missingUserId() throws IOException {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add("clientId=urn%3Aebay-marketplace-consumerid%3A409a9203-ad93-4932-a101-485637812d0d,ip=10.224.57.159,userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage%2Fapng%2C*%2F*%3Bq%3D0.8%2Capplication%2Fsigned-exchange%3Bv%3Db3,userAgentAcceptEncoding=gzip%2C+deflate,userAgent=Mozilla%2F5.0+(Macintosh%3B+Intel+Mac+OS+X+10_14_6)+AppleWebKit%2F537.36+(KHTML%2C+like+Gecko)+Chrome%2F78.0.3904.87+ Safari%2F537.36,uri=%2F,applicationURL=http%3A%2F%2Flvsadtrackhandlersample-3711473.lvs02.dev.ebayc3.com%3A8080%2F%3Famdata%3D%257B%2522enc%2522%253A%2522AQAFAAACIJTCObh01lRshq6SSqqfH3WQwGz0UI8axZzmevNEvXI54PLKnoki9RTrH7VxhIKdqX9GysFTjSpZZBS%252F7ijIy9J7ewapJiBi8EZr%252F6GjI%252BLJ0lnJ4H16uClzxxPY9qXRsHrp0BFJ8GrQlf3FtCgYLQ9F5iQq38ayx2ib6%252Fe73QxNqupgEJYLTkEQr08ea2%252BJvUvzkyNRZpiajjSAaur%252FqOk9%252FS879Mc%252BpCeV%252BaS%252F0zgj1KFAhuabi9pS%252FKMjnK3fm8DIil3pbGL96CvSWFqJp5PwYfSphP2lcyuobp2In2MwF8EF3Swp6T8bGQ0gPxJGMCbCZ3xs1h2O8vVTntN0hhr9wJfaoEC5WQzNnxxZYh6Tab9R%252FEZpJxBJjOkQomi6RG5hQJtLAkQM%252B88zaOkn0pxno7m70PWG6lQmjrQRn%252BshJmzev%252BwLan2x4%252B5mIMycD6fql3LTyaEzSVzFn8XUYDXQTlaKNSAl%252BjFnmGYFTM6VsX8PSd2mKSwnETzVsZZxuxS3eNlHxOvpUDpMZh54EaY9TRK%252FqgES3wYjrmqA57e6HkAEpi5IWb47q2LM3lxytCe0LGSYzmpM3lGMrfGd88Gy0PdGv2kJq%252FBwZofCwBhocJgjS7z66A%252Bm0dRb4cBV0s0V9Itj6Cq9GcY8ADhsARpf%252F6YrGnnBQYZYQcs%252BUeTe0zV7hw2qzo85LgqX%252BNgG7gfDc7gkp0Yi3YOZCPa3ogU%253D%2522%252C%2522cksum%2522%253A%2520%2522checksum%2520of%2520enc%2522%252C%2522ampid%2522%253A%2520%2522PL_CLK%2522%252C%2522clp%2522%253A%2520512325%252C%2522ekp%2522%253A%2520true%257D,expectSecureURL=false,physicalLocation=country%3DUS");
        headers.put("X-EBAY-C-ENDUSERCTX",list1);
        List<String> list2 = new ArrayList<>();
        list2.add("guid=8b99eec616c0a9ccdb932f64fcc116b3,cguid=8b99eed116c0a9ccdb932f64fcc116b1,tguid=8b99eec616c0a9ccdb932f64fcc116b3,pageid=2562720,cobrandId=2");
        headers.put("X-EBAY-C-TRACKING",list2);
        String endUserContextValue = adsCollectionSvcClient.addAdditionalValuesToEndUserCtxHeader(headers, "testuserid", null);
        assertTrue(endUserContextValue.contains("origUserId=origAcctId%3Dtestuserid"));
    }

    @Test
    public void addUserIdToRequestHeader_noheader() throws IOException {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        List<String> list2 = new ArrayList<>();
        list2.add("guid=8b99eec616c0a9ccdb932f64fcc116b3,cguid=8b99eed116c0a9ccdb932f64fcc116b1,tguid=8b99eec616c0a9ccdb932f64fcc116b3,pageid=2562720,cobrandId=2");
        headers.put("X-EBAY-C-TRACKING",list2);
        String endUserContextValue = adsCollectionSvcClient.addAdditionalValuesToEndUserCtxHeader(headers, "testuserid", null);
        assertTrue(endUserContextValue.contains("origUserId=origAcctId%3Dtestuserid"));
    }

    @Test
    public void addUserIdToRequestHeader_containUserId() throws IOException {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add("clientId=urn%3Aebay-marketplace-consumerid%3A409a9203-ad93-4932-a101-485637812d0d,ip=10.224.57.159,userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage%2Fapng%2C*%2F*%3Bq%3D0.8%2Capplication%2Fsigned-exchange%3Bv%3Db3,userAgentAcceptEncoding=gzip%2C+deflate,userAgent=Mozilla%2F5.0+(Macintosh%3B+Intel+Mac+OS+X+10_14_6)+AppleWebKit%2F537.36+(KHTML%2C+like+Gecko)+Chrome%2F78.0.3904.87+ Safari%2F537.36,uri=%2F,applicationURL=http%3A%2F%2Flvsadtrackhandlersample-3711473.lvs02.dev.ebayc3.com%3A8080%2F%3Famdata%3D%257B%2522enc%2522%253A%2522AQAFAAACIJTCObh01lRshq6SSqqfH3WQwGz0UI8axZzmevNEvXI54PLKnoki9RTrH7VxhIKdqX9GysFTjSpZZBS%252F7ijIy9J7ewapJiBi8EZr%252F6GjI%252BLJ0lnJ4H16uClzxxPY9qXRsHrp0BFJ8GrQlf3FtCgYLQ9F5iQq38ayx2ib6%252Fe73QxNqupgEJYLTkEQr08ea2%252BJvUvzkyNRZpiajjSAaur%252FqOk9%252FS879Mc%252BpCeV%252BaS%252F0zgj1KFAhuabi9pS%252FKMjnK3fm8DIil3pbGL96CvSWFqJp5PwYfSphP2lcyuobp2In2MwF8EF3Swp6T8bGQ0gPxJGMCbCZ3xs1h2O8vVTntN0hhr9wJfaoEC5WQzNnxxZYh6Tab9R%252FEZpJxBJjOkQomi6RG5hQJtLAkQM%252B88zaOkn0pxno7m70PWG6lQmjrQRn%252BshJmzev%252BwLan2x4%252B5mIMycD6fql3LTyaEzSVzFn8XUYDXQTlaKNSAl%252BjFnmGYFTM6VsX8PSd2mKSwnETzVsZZxuxS3eNlHxOvpUDpMZh54EaY9TRK%252FqgES3wYjrmqA57e6HkAEpi5IWb47q2LM3lxytCe0LGSYzmpM3lGMrfGd88Gy0PdGv2kJq%252FBwZofCwBhocJgjS7z66A%252Bm0dRb4cBV0s0V9Itj6Cq9GcY8ADhsARpf%252F6YrGnnBQYZYQcs%252BUeTe0zV7hw2qzo85LgqX%252BNgG7gfDc7gkp0Yi3YOZCPa3ogU%253D%2522%252C%2522cksum%2522%253A%2520%2522checksum%2520of%2520enc%2522%252C%2522ampid%2522%253A%2520%2522PL_CLK%2522%252C%2522clp%2522%253A%2520512325%252C%2522ekp%2522%253A%2520true%257D,expectSecureURL=false,physicalLocation=country%3DUS,origAcctId=baduserid");
        headers.put("X-EBAY-C-ENDUSERCTX",list1);
        List<String> list2 = new ArrayList<>();
        list2.add("guid=8b99eec616c0a9ccdb932f64fcc116b3,cguid=8b99eed116c0a9ccdb932f64fcc116b1,tguid=8b99eec616c0a9ccdb932f64fcc116b3,pageid=2562720,cobrandId=2");
        headers.put("X-EBAY-C-TRACKING",list2);
        String endUserContextValue = adsCollectionSvcClient.addAdditionalValuesToEndUserCtxHeader(headers, "testuserid", null);
        assertTrue(endUserContextValue.contains("origUserId=origAcctId%3Dtestuserid"));
    }

    @Test
    public void addUserIdToHeaderProcessHeaderValues() throws IOException {
        RaptorSecureContextProvider raptorSecureContextProvider = mock(RaptorSecureContextProvider.class);
        RaptorSecureContext raptorSecureContext = mock(RaptorSecureContext.class);
        when(raptorSecureContextProvider.get()).thenReturn(raptorSecureContext);
        when(raptorSecureContext.getSubjectImmutableId()).thenReturn("testtestuserid");
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        List<String> list2 = new ArrayList<>();
        list2.add("guid=8b99eec616c0a9ccdb932f64fcc116b3,cguid=8b99eed116c0a9ccdb932f64fcc116b1,tguid=8b99eec616c0a9ccdb932f64fcc116b3,pageid=2562720,cobrandId=2");
        headers.put("X-EBAY-C-TRACKING",list2);
        adsCollectionSvcClient.raptorSecureContextProvider = raptorSecureContextProvider;
        assertTrue(adsCollectionSvcClient.processEndUserContext("https://www.ebay.com/", headers).contains("origUserId=origAcctId%3Dtesttestuserid"));
    }

    @Test
    public void addUserIdToRequestHeader_missingUserId_missingreferal() throws IOException {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add("clientId=urn%3Aebay-marketplace-consumerid%3A409a9203-ad93-4932-a101-485637812d0d,ip=10.224.57.159,userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage%2Fapng%2C*%2F*%3Bq%3D0.8%2Capplication%2Fsigned-exchange%3Bv%3Db3,userAgentAcceptEncoding=gzip%2C+deflate,userAgent=Mozilla%2F5.0+(Macintosh%3B+Intel+Mac+OS+X+10_14_6)+AppleWebKit%2F537.36+(KHTML%2C+like+Gecko)+Chrome%2F78.0.3904.87+ Safari%2F537.36,uri=%2F,applicationURL=http%3A%2F%2Flvsadtrackhandlersample-3711473.lvs02.dev.ebayc3.com%3A8080%2F%3Famdata%3D%257B%2522enc%2522%253A%2522AQAFAAACIJTCObh01lRshq6SSqqfH3WQwGz0UI8axZzmevNEvXI54PLKnoki9RTrH7VxhIKdqX9GysFTjSpZZBS%252F7ijIy9J7ewapJiBi8EZr%252F6GjI%252BLJ0lnJ4H16uClzxxPY9qXRsHrp0BFJ8GrQlf3FtCgYLQ9F5iQq38ayx2ib6%252Fe73QxNqupgEJYLTkEQr08ea2%252BJvUvzkyNRZpiajjSAaur%252FqOk9%252FS879Mc%252BpCeV%252BaS%252F0zgj1KFAhuabi9pS%252FKMjnK3fm8DIil3pbGL96CvSWFqJp5PwYfSphP2lcyuobp2In2MwF8EF3Swp6T8bGQ0gPxJGMCbCZ3xs1h2O8vVTntN0hhr9wJfaoEC5WQzNnxxZYh6Tab9R%252FEZpJxBJjOkQomi6RG5hQJtLAkQM%252B88zaOkn0pxno7m70PWG6lQmjrQRn%252BshJmzev%252BwLan2x4%252B5mIMycD6fql3LTyaEzSVzFn8XUYDXQTlaKNSAl%252BjFnmGYFTM6VsX8PSd2mKSwnETzVsZZxuxS3eNlHxOvpUDpMZh54EaY9TRK%252FqgES3wYjrmqA57e6HkAEpi5IWb47q2LM3lxytCe0LGSYzmpM3lGMrfGd88Gy0PdGv2kJq%252FBwZofCwBhocJgjS7z66A%252Bm0dRb4cBV0s0V9Itj6Cq9GcY8ADhsARpf%252F6YrGnnBQYZYQcs%252BUeTe0zV7hw2qzo85LgqX%252BNgG7gfDc7gkp0Yi3YOZCPa3ogU%253D%2522%252C%2522cksum%2522%253A%2520%2522checksum%2520of%2520enc%2522%252C%2522ampid%2522%253A%2520%2522PL_CLK%2522%252C%2522clp%2522%253A%2520512325%252C%2522ekp%2522%253A%2520true%257D,expectSecureURL=false,physicalLocation=country%3DUS");
        headers.put("X-EBAY-C-ENDUSERCTX",list1);
        List<String> list2 = new ArrayList<>();
        list2.add("guid=8b99eec616c0a9ccdb932f64fcc116b3,cguid=8b99eed116c0a9ccdb932f64fcc116b1,tguid=8b99eec616c0a9ccdb932f64fcc116b3,pageid=2562720,cobrandId=2");
        headers.put("X-EBAY-C-TRACKING",list2);
        String endUserContextValue = adsCollectionSvcClient.addAdditionalValuesToEndUserCtxHeader(headers, null, null);
        assertFalse(endUserContextValue != null);
        assertFalse(endUserContextValue != null);
    }
    @Test
    public void addUserIdToRequestHeader_missingUserId_missingreferal_emptyString() throws IOException {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add("clientId=urn%3Aebay-marketplace-consumerid%3A409a9203-ad93-4932-a101-485637812d0d,ip=10.224.57.159,userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage%2Fapng%2C*%2F*%3Bq%3D0.8%2Capplication%2Fsigned-exchange%3Bv%3Db3,userAgentAcceptEncoding=gzip%2C+deflate,userAgent=Mozilla%2F5.0+(Macintosh%3B+Intel+Mac+OS+X+10_14_6)+AppleWebKit%2F537.36+(KHTML%2C+like+Gecko)+Chrome%2F78.0.3904.87+ Safari%2F537.36,uri=%2F,applicationURL=http%3A%2F%2Flvsadtrackhandlersample-3711473.lvs02.dev.ebayc3.com%3A8080%2F%3Famdata%3D%257B%2522enc%2522%253A%2522AQAFAAACIJTCObh01lRshq6SSqqfH3WQwGz0UI8axZzmevNEvXI54PLKnoki9RTrH7VxhIKdqX9GysFTjSpZZBS%252F7ijIy9J7ewapJiBi8EZr%252F6GjI%252BLJ0lnJ4H16uClzxxPY9qXRsHrp0BFJ8GrQlf3FtCgYLQ9F5iQq38ayx2ib6%252Fe73QxNqupgEJYLTkEQr08ea2%252BJvUvzkyNRZpiajjSAaur%252FqOk9%252FS879Mc%252BpCeV%252BaS%252F0zgj1KFAhuabi9pS%252FKMjnK3fm8DIil3pbGL96CvSWFqJp5PwYfSphP2lcyuobp2In2MwF8EF3Swp6T8bGQ0gPxJGMCbCZ3xs1h2O8vVTntN0hhr9wJfaoEC5WQzNnxxZYh6Tab9R%252FEZpJxBJjOkQomi6RG5hQJtLAkQM%252B88zaOkn0pxno7m70PWG6lQmjrQRn%252BshJmzev%252BwLan2x4%252B5mIMycD6fql3LTyaEzSVzFn8XUYDXQTlaKNSAl%252BjFnmGYFTM6VsX8PSd2mKSwnETzVsZZxuxS3eNlHxOvpUDpMZh54EaY9TRK%252FqgES3wYjrmqA57e6HkAEpi5IWb47q2LM3lxytCe0LGSYzmpM3lGMrfGd88Gy0PdGv2kJq%252FBwZofCwBhocJgjS7z66A%252Bm0dRb4cBV0s0V9Itj6Cq9GcY8ADhsARpf%252F6YrGnnBQYZYQcs%252BUeTe0zV7hw2qzo85LgqX%252BNgG7gfDc7gkp0Yi3YOZCPa3ogU%253D%2522%252C%2522cksum%2522%253A%2520%2522checksum%2520of%2520enc%2522%252C%2522ampid%2522%253A%2520%2522PL_CLK%2522%252C%2522clp%2522%253A%2520512325%252C%2522ekp%2522%253A%2520true%257D,expectSecureURL=false,physicalLocation=country%3DUS");
        headers.put("X-EBAY-C-ENDUSERCTX",list1);
        List<String> list2 = new ArrayList<>();
        list2.add("guid=8b99eec616c0a9ccdb932f64fcc116b3,cguid=8b99eed116c0a9ccdb932f64fcc116b1,tguid=8b99eec616c0a9ccdb932f64fcc116b3,pageid=2562720,cobrandId=2");
        headers.put("X-EBAY-C-TRACKING",list2);
        String endUserContextValue = adsCollectionSvcClient.addAdditionalValuesToEndUserCtxHeader(headers, "", " ");
        assertFalse(endUserContextValue != null);
        assertFalse(endUserContextValue != null);
    }


    /**
     * User id is NOT null but referer is null
     * @throws IOException
     */
    @Test
    public void addRefererToRequestHeader_containReferer() throws IOException {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add("clientId=urn%3Aebay-marketplace-consumerid%3A409a9203-ad93-4932-a101-485637812d0d,ip=10.224.57.159,userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage%2Fapng%2C*%2F*%3Bq%3D0.8%2Capplication%2Fsigned-exchange%3Bv%3Db3,userAgentAcceptEncoding=gzip%2C+deflate,userAgent=Mozilla%2F5.0+(Macintosh%3B+Intel+Mac+OS+X+10_14_6)+AppleWebKit%2F537.36+(KHTML%2C+like+Gecko)+Chrome%2F78.0.3904.87+ Safari%2F537.36,uri=%2F,applicationURL=http%3A%2F%2Flvsadtrackhandlersample-3711473.lvs02.dev.ebayc3.com%3A8080%2F%3Famdata%3D%257B%2522enc%2522%253A%2522AQAFAAACIJTCObh01lRshq6SSqqfH3WQwGz0UI8axZzmevNEvXI54PLKnoki9RTrH7VxhIKdqX9GysFTjSpZZBS%252F7ijIy9J7ewapJiBi8EZr%252F6GjI%252BLJ0lnJ4H16uClzxxPY9qXRsHrp0BFJ8GrQlf3FtCgYLQ9F5iQq38ayx2ib6%252Fe73QxNqupgEJYLTkEQr08ea2%252BJvUvzkyNRZpiajjSAaur%252FqOk9%252FS879Mc%252BpCeV%252BaS%252F0zgj1KFAhuabi9pS%252FKMjnK3fm8DIil3pbGL96CvSWFqJp5PwYfSphP2lcyuobp2In2MwF8EF3Swp6T8bGQ0gPxJGMCbCZ3xs1h2O8vVTntN0hhr9wJfaoEC5WQzNnxxZYh6Tab9R%252FEZpJxBJjOkQomi6RG5hQJtLAkQM%252B88zaOkn0pxno7m70PWG6lQmjrQRn%252BshJmzev%252BwLan2x4%252B5mIMycD6fql3LTyaEzSVzFn8XUYDXQTlaKNSAl%252BjFnmGYFTM6VsX8PSd2mKSwnETzVsZZxuxS3eNlHxOvpUDpMZh54EaY9TRK%252FqgES3wYjrmqA57e6HkAEpi5IWb47q2LM3lxytCe0LGSYzmpM3lGMrfGd88Gy0PdGv2kJq%252FBwZofCwBhocJgjS7z66A%252Bm0dRb4cBV0s0V9Itj6Cq9GcY8ADhsARpf%252F6YrGnnBQYZYQcs%252BUeTe0zV7hw2qzo85LgqX%252BNgG7gfDc7gkp0Yi3YOZCPa3ogU%253D%2522%252C%2522cksum%2522%253A%2520%2522checksum%2520of%2520enc%2522%252C%2522ampid%2522%253A%2520%2522PL_CLK%2522%252C%2522clp%2522%253A%2520512325%252C%2522ekp%2522%253A%2520true%257D,expectSecureURL=false,physicalLocation=country%3DUS,referer=badreferer");
        headers.put("X-EBAY-C-ENDUSERCTX",list1);
        List<String> list2 = new ArrayList<>();
        list2.add("guid=8b99eec616c0a9ccdb932f64fcc116b3,cguid=8b99eed116c0a9ccdb932f64fcc116b1,tguid=8b99eec616c0a9ccdb932f64fcc116b3,pageid=2562720,cobrandId=2");
        headers.put("X-EBAY-C-TRACKING",list2);
        String endUserContextValue = adsCollectionSvcClient.addAdditionalValuesToEndUserCtxHeader(headers, "", "http://www.tesla.com");
        assertTrue(endUserContextValue.contains("referer=http%3A%2F%2Fwww.tesla.com"));
    }

    @Test
    public void refereroRequestHeader_noheader() throws IOException {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        List<String> list2 = new ArrayList<>();
        list2.add("guid=8b99eec616c0a9ccdb932f64fcc116b3,cguid=8b99eed116c0a9ccdb932f64fcc116b1,tguid=8b99eec616c0a9ccdb932f64fcc116b3,pageid=2562720,cobrandId=2");
        headers.put("X-EBAY-C-TRACKING",list2);
        String endUserContextValue = adsCollectionSvcClient.addAdditionalValuesToEndUserCtxHeader(headers, null, "http://www.tesla.com");
        assertTrue(endUserContextValue.contains("referer=http%3A%2F%2Fwww.tesla.com"));
    }

    @Test
    public void addRefererToHeaderProcessHeaderValues() throws IOException {
        RaptorSecureContextProvider raptorSecureContextProvider = mock(RaptorSecureContextProvider.class);
        RaptorSecureContext raptorSecureContext = mock(RaptorSecureContext.class);
        when(raptorSecureContextProvider.get()).thenReturn(raptorSecureContext);
        when(raptorSecureContext.getSubjectImmutableId()).thenReturn("testtestuserid");
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        List<String> list2 = new ArrayList<>();
        list2.add("guid=8b99eec616c0a9ccdb932f64fcc116b3,cguid=8b99eed116c0a9ccdb932f64fcc116b1,tguid=8b99eec616c0a9ccdb932f64fcc116b3,pageid=2562720,cobrandId=2");
        headers.put("X-EBAY-C-TRACKING",list2);
        adsCollectionSvcClient.raptorSecureContextProvider = raptorSecureContextProvider;
        String endUserCtxt = adsCollectionSvcClient.processEndUserContext("http://www.tesla.com", headers);
        assertTrue(endUserCtxt.contains("origUserId=origAcctId%3Dtesttestuserid"));
        assertTrue(endUserCtxt.contains("referer=http%3A%2F%2Fwww.tesla.com"));
    }

    /**
     * Both User id and t referer are NOT null
     * @throws IOException
     */
    @Test
    public void addRefererUserIdToRequestHeader() throws IOException {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add("deviceId=17034ee26ab.aa6a248.4f216.ffff14ec,deviceIdType=IDREF,userAgent=ebayUserAgent/eBayIOS;6.7.0;iOS;13.6.1;Apple;iPhone10_4;Three;375x667;2.0");
        headers.put("X-EBAY-C-ENDUSERCTX",list1);
        List<String> list2 = new ArrayList<>();
        list2.add("guid=8b99eec616c0a9ccdb932f64fcc116b3,cguid=8b99eed116c0a9ccdb932f64fcc116b1,tguid=8b99eec616c0a9ccdb932f64fcc116b3,pageid=2562720,cobrandId=2");
        headers.put("X-EBAY-C-TRACKING",list2);
        String endUserContextValue = adsCollectionSvcClient.addAdditionalValuesToEndUserCtxHeader(headers, "testtestuserid", "http://www.tesla.com");
        assertTrue(endUserContextValue.contains("referer=http%3A%2F%2Fwww.tesla.com"));
        assertTrue(endUserContextValue.contains("origUserId=origAcctId%3Dtesttestuserid"));
        assertEquals("deviceId=17034ee26ab.aa6a248.4f216.ffff14ec,deviceIdType=IDREF,userAgent=ebayUserAgent/eBayIOS;6.7.0;iOS;13.6.1;Apple;iPhone10_4;Three;375x667;2.0,origUserId=origAcctId%3Dtesttestuserid,referer=http%3A%2F%2Fwww.tesla.com", endUserContextValue);
    }

    /**
     * Both User id and t referer are NOT null
     * @throws IOException
     */
    @Test
    public void addUserIdToRequestHeader_noreferer() throws IOException {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add("deviceId=17034ee26ab.aa6a248.4f216.ffff14ec,deviceIdType=IDREF,userAgent=ebayUserAgent/eBayIOS;6.7.0;iOS;13.6.1;Apple;iPhone10_4;Three;375x667;2.0");
        headers.put("X-EBAY-C-ENDUSERCTX",list1);
        List<String> list2 = new ArrayList<>();
        list2.add("guid=8b99eec616c0a9ccdb932f64fcc116b3,cguid=8b99eed116c0a9ccdb932f64fcc116b1,tguid=8b99eec616c0a9ccdb932f64fcc116b3,pageid=2562720,cobrandId=2");
        headers.put("X-EBAY-C-TRACKING",list2);
        String endUserContextValue = adsCollectionSvcClient.addAdditionalValuesToEndUserCtxHeader(headers, "testtestuserid", "");
        assertFalse(endUserContextValue.contains("referer=http%3A%2F%2Fwww.tesla.com"));
        assertTrue(endUserContextValue.contains("origUserId=origAcctId%3Dtesttestuserid"));
        assertEquals("deviceId=17034ee26ab.aa6a248.4f216.ffff14ec,deviceIdType=IDREF,userAgent=ebayUserAgent/eBayIOS;6.7.0;iOS;13.6.1;Apple;iPhone10_4;Three;375x667;2.0,origUserId=origAcctId%3Dtesttestuserid", endUserContextValue);
    }

    @Test
    public void headerProcess(){
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("referrer", "https://www.google.com");
        MultivaluedMap<String,Object> processedHeader = adsCollectionSvcClient.generateSvcHeader(headers, "");
        assertNotNull(processedHeader.get("referrer"));

    }
}
