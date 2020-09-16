package com.ebay.traffic.chocolate.listener.component;

import com.ebay.platform.security.nextgen.domain.SecretResponse;
import com.ebay.platform.security.trustfabric.client.TfTokenClient;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Objects;

/**
 * @Author YangYang
 */
@Component
public class FideliusClient {
    private static final String URI_OBJECTS = "/objects/chocolis/";

    @Autowired
    private TfTokenClient tfTokenClient;

    @Autowired
    @Qualifier("fidelius.vault")
    private WebTarget webTarget;

    /**
     * Create a secret in the specified path in Fidelius.
     *
     * @param path   Path is a combination of appName/folderName/secretName
     * @param secret Value of the secret
     */
    public void create(String path, String secret) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "path cannot be blank!");
        Preconditions.checkArgument(!Objects.isNull(secret), "secret cannot be null!");
        FideliusRequest fideliusRequest = new FideliusRequest(secret);
        Response response = webTarget.path(URI_OBJECTS + path).request().header(HttpHeaders.AUTHORIZATION, getTfToken()).put(Entity.entity(fideliusRequest, MediaType.APPLICATION_JSON));
        checkError(response);
    }

    /**
     * Get the secret from the path from Fidelius.
     *
     * @param path Path is a combination of appName/folderName/secretName
     * @return The secret value
     */
    public String getContent(String path) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "path cannot be blank!");
        Response response = webTarget.path(URI_OBJECTS + path).request()
                .header(HttpHeaders.AUTHORIZATION, getTfToken()).get();
        checkError(response);
        SecretResponse secretResponse = response.readEntity(SecretResponse.class);
        return secretResponse.getValue();
    }

    /**
     * Check for the availability of the current TF token and return the Bearer access token.
     *
     * @return TF token prefixed with Bearer
     */
    public String getTfToken() {
        if (tfTokenClient.isTokenAvailable()) {
            return tfTokenClient.getTokenWithBearerPrefix();
        }
        throw new RuntimeException("TrustFabric token not available for Fidelius client calls");
    }

    /**
     * Check for all non 2XX errors from Fidelius. Fidelius give proper 4XX, 5XX error responses for the failed calls
     * with an error message, status code and error reason.
     *
     * @param response any non 2XX errors
     */
    private void checkError(Response response) {
        if (response.getStatus() != HttpStatus.OK.value()) {
            // Read it as a string if detail un-marshalled error information is not required
            String errorResponse = response.readEntity(String.class);
            throw new RuntimeException(errorResponse);
        }
    }

    /**
     * Delete the secret from the specified path.
     *
     * @param path Path is a combination of appName/folderName/secretName
     */
    public void delete(String path) {
        Preconditions.checkArgument(StringUtils.isNotBlank(path), "path cannot be blank!");
        Response response = webTarget.path(URI_OBJECTS + path).request()
                .header(HttpHeaders.AUTHORIZATION, getTfToken()).delete();
        checkError(response);
    }
}
