package com.ebay.fount.fountclient.http;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.net.ssl.SSLSocketFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ebay.fount.fountclient.config.FountClientConfig;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;

import com.ebay.fount.fountclient.FountClientException;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Scanner;

import java.net.URLEncoder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import org.json.JSONObject;

public class FountClientHttpHelper {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(FountClientHttpHelper.class);

    public static final String FOUNT_CLIENT_EVENT = "Fount.Client";

    static final String REQUEST_HEADER_IDENTITY_NAME = "X-EBAY-DELIVERABLE-IDENTITY";
    static final String REQUEST_HEADER_CLIENT_IDENTIFIER = "X-FOUNTCSVC-CLIENT";
    //	static final String REQUEST_HEADER_CLIENT_IDENTIFIER_VALUE = "fountclient 0.0.0";
    private static final String clientVersion = getVersion(new Object(){}.getClass().getPackage());

    //	static Proxy proxy = null;
    static final SSLSocketFactory sslSocketFactory = SSLSocketFactoryHelper.getSSLSocketFactory();
//	static HostnameVerifier hostnameVerifier = null;

//	private static volatile String esamsIdentity;

    private static final HttpTransport httpTransport = new NetHttpTransport.Builder()
//			.setProxy(proxy)//.setHostnameVerifier(hostnameVerifier)
            .setSslSocketFactory(sslSocketFactory).build();

    private final Map<String,String> staticHeaders = new HashMap<>();

    private HttpRequestFactory requestFactory = null;

    protected int maxRetryCount;

    protected int connectTimeout;
    protected int readTimeout;
    protected int numberOfRetries;

    protected int errorCountThreshold = 0;

    private final IIdentityGenerator trustFabricIdentityGenerator;
    private final IIdentityGenerator esamsIdentityGenerator;

    static {
        LOGGER.warn("client version: " + clientVersion);
    }

    public FountClientHttpHelper(
            final IIdentityGenerator trustFabricIdentityGenerator,
            final IIdentityGenerator esamsIdentityGenerator)
    {
        this.trustFabricIdentityGenerator = trustFabricIdentityGenerator;
        this.esamsIdentityGenerator = esamsIdentityGenerator;
        setRequestFactory(null);
    }

    public HttpRequestFactory getRequestFactory() {
        return requestFactory;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

//	static void initEsamsIdentity() {
//		try {
//			String strMsg = "Initializing FOUNT REST Client.";
//
//			// Get the ESAMS Identity file used for FOUNT authentication.
//			final IdentityProvider instance = IdentityProvider.getInstance();
//			final String identity = instance.getIdentity();
//
//			if (identity != null) {
//				esamsIdentity = DatatypeConverter.printBase64Binary(identity.getBytes("UTF-8"));
//			} else {
//				strMsg = strMsg + " - Missing Identity information.";
//			}
//			// Log FOUNT Client Initialization.
//			LOGGER.warn(FOUNT_CLIENT_EVENT, strMsg);
//
//		} catch (Exception e) {
//			final String strMsg = "Error during FOUNT Client creation.";
//			// Log Exception to CAL... Don't fail class initialization if we are
//			// unable to read ESAMS information.
//			LOGGER.error(strMsg, e.getMessage());
//		}
//	}

    public void restart() throws IOException {

        httpTransport.shutdown();

        setRequestFactory(null);
    }

    public String createTokenForTess(){
        LOGGER.warn("Enter into create Token for Tess");
        String checkTfTokeninFount=System.getenv("CHECK_TF_TOKEN_IN_FOUNT");
        System.out.println("set CHECK_TF_TOKEN_IN_FOUNT as true");
        checkTfTokeninFount = "true";
        if(checkTfTokeninFount==null){
            LOGGER.warn("Check Trust Fabric token in tess is null, or this is not tess environment.");
            return null;
        }
        if(!checkTfTokeninFount.equals("true")){
            LOGGER.warn("Check Trust Fabric token in tess is off, or this is not tess environment.");
            return null;
        }
        String podName = System.getenv("POD_NAME");
        String path=System.getenv("TRF_GRANT_FILE");
        if(path==null||path.equals("")){
            LOGGER.info("Get TRF_GRANT_FILE from /var/run/secrets/trustfabric/trustfabric_%s.cg");
            path = String.format("/var/run/secrets/trustfabric/trustfabric_%s.cg", podName);
        }else{
            LOGGER.info("Get TRF_GRANT_FILE from env variable TRF_GRANT_FILE");
        }

        //String path = String.format("/var/run/secrets/trustfabric/trustfabric_%s.cg", podName);
        File file = new File(path);
        if (!file.exists()) {
            LOGGER.warn("No pod file, fail to create token in tess");
            return null;
        }
        InputStream stream=null;
        try {
            stream = new FileInputStream(file);
        }catch (Exception e){
            LOGGER.warn("No InputStream, fail to create token in tess");
            return null;
        }
        String grant="";
        try{
            Scanner sc = new Scanner(stream);
            //Reading line by line from scanner to StringBuffer
            StringBuffer sb = new StringBuffer();

            while(sc.hasNext()){
                sb.append(sc.nextLine());
            }
            grant = sb.toString();
        } catch(Exception e){
            LOGGER.warn("Parse input file fails, fail to create token in tess");
            return null;
        }
        // System.out.println(sb.toString());
        String appInstName = System.getenv("APP_INSTANCE_NAME");
        String appName = System.getenv("APP_NAME");
        String appEnv = System.getenv("APP_ENV");
        if(appInstName==null||appName==null||appEnv==null){
            LOGGER.warn("clientID info missing, fail to create token in tess");
        }
        LOGGER.info("appInstName " + appInstName + " appName " + appName + " appEnv " + appEnv);
        String clientId = String.format("ou=%s+l=%s,o=%s,dc=tess,dc=ebay,dc=com", appInstName, appEnv, appName);
        LOGGER.info(clientId+"this is clientID");
        String encoded="";

        try{
            encoded = URLEncoder.encode(clientId,"UTF-8");
        }catch(Exception e){
            LOGGER.warn("Fail to encode");
            return null;
        }
        HttpEntity entity=null;
        String result="";
        try{
            String body = String.format("client_id=%s&grant_type=authorization_code&code=%s", encoded, grant);
            HttpClient httpclient = new DefaultHttpClient();
            // System.out.println("https://trustfabric.vip.ebay.com/v2/token?"+body);
            HttpPost httppost = new HttpPost("https://trustfabric.vip.ebay.com/v2/token?"+body);
            HttpResponse response = httpclient.execute(httppost);
            entity = response.getEntity();
            String entityString=EntityUtils.toString(entity, "UTF-8");
            if(entityString==null){
                LOGGER.warn("entityString is null");
                return null;
            }
            else{
                JSONObject entityJson = new JSONObject(entityString); //Convert String to JSON Object
                result = "Bearer "+entityJson.getString("access_token");
                // System.out.println(result);
            }
        }catch(Exception e){
            LOGGER.warn("fail to call trustfabric.vip.ebay.com"+e.getMessage());
            return null;
        }

        return result;
    }

    public void setRequestFactory(HttpRequestFactory httpRequestFactory) {

        this.connectTimeout = FountClientConfig.getInstance().getFountRestConnectTimeout();
        this.readTimeout = FountClientConfig.getInstance().getFountRestReadTimeout();
        this.errorCountThreshold = FountClientConfig.getInstance().getFountRestErrorCountThreshold();
        this.maxRetryCount = FountClientConfig.getInstance().getFountRestMaxRetryCount();
        this.numberOfRetries = FountClientConfig.getInstance().getFountRestNumberOfRetries();

//		initEsamsIdentity();

        if (httpRequestFactory != null) {
            requestFactory = httpRequestFactory;
        } else {
            final String esamsIdentity = esamsIdentityGenerator.genIdentity();
            if (esamsIdentity != null) {
                staticHeaders.put(REQUEST_HEADER_IDENTITY_NAME, esamsIdentity);
            }
            staticHeaders.put(REQUEST_HEADER_CLIENT_IDENTIFIER, clientVersion);

            requestFactory = httpTransport.createRequestFactory(
                    new HttpRequestInitializer() {
                        @Override
                        public void initialize(final HttpRequest request) {
                            request.setConnectTimeout(connectTimeout);
                            request.setReadTimeout(readTimeout);
                            request.setNumberOfRetries(numberOfRetries);
                            final HttpHeaders headers = request.getHeaders();
                            for (final Entry<String,String> entry:staticHeaders.entrySet()) {
                                headers.set(entry.getKey(), entry.getValue());
                            }

                            // trust fabric tokens are only good for a very short
                            // time, the trust fabric javadoc says not to cache;
                            // hence, need to ask for a new token each time.
                            String authVal =null;
                            try{
                                authVal = trustFabricIdentityGenerator.genIdentity();
                            }catch (Exception e){
                                authVal=createTokenForTess();
                            }
                            headers.set("authorization", Arrays.asList(authVal));
                        }
                    });
        }
    }

    static String getVersion(final Package objPackage) {
        if (objPackage == null) {
            return "fount-client 0.0.0";
        }
        final String impVersion = objPackage.getImplementationVersion();
        if (impVersion != null) {
            final String cleanImpVersion = impVersion.trim();
            if (!cleanImpVersion.isEmpty()) {
                return "fount-client " + cleanImpVersion;
            }
        }

        final String specVersion = objPackage.getSpecificationVersion();
        if (specVersion == null) {
            return "fount-client 0.0.0";
        }
        final String cleanSpecVersion = specVersion.trim();
        if (!cleanSpecVersion.isEmpty()) {
            return "font-client " + cleanSpecVersion;
        }
        return "fount-client 0.0.0";
    }

}
