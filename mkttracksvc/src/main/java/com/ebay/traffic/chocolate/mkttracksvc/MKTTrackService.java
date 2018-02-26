package com.ebay.traffic.chocolate.mkttracksvc;

import com.ebay.cos.raptor.service.annotations.Api;
import com.ebay.cos.raptor.service.annotations.Service;
import com.ebay.cos.raptor.service.annotations.UriPatternEnum;

/**
 * This @Service annotated interface is used by the SDD Generator to automatically produce documentation
 * which describes your service and the Apis which it supports.
 * <p>
 * ADDITIONAL LINKS:
 * <p>
 * https://github.corp.ebay.com/commerceos/standards-annotation-component/blob/master/service-descriptor-annotations.md
 * <p>
 * https://github.corp.ebay.com/commerceos/cos-naming-conventions/blob/master/naming-examples.md
 * https://github.corp.ebay.com/commerceos/cos-idm/blob/master/idm.md
 */
@Service(

    // Human Readable project name.
    // Used with Version to distinctly identify your project in Fulcrum.
    // format: Letters, Numbers and Spaces
    name = "Marketing Tracking Service",

    // Service Version as reported in Fulcrum.
    // This is *not* the same as your pom/version.
    // format:  ##.##.## = Major.Minor.Maintenance
    version = "1.0.0",

    // Human Readable description of Service.
    // This long explanation will be presented in external documentation.
    description = "Provide the service to generate tracking info",

    api = {

        // First Api supported by this Service.
        // When you create additional APIs, or add versions to an API, create additional entries here.
        @Api(

            // Context and Name of this service organized as either ( 'context' or 'context/name' )
            // The context and name for your service will be finalized during COPR review.
            // format:  Lowercase letters and numbers, single words ( no special characters )
            name = "tracksvc",

            // title ::: short, human readable description of the service
            title = "marketing track service",

            // version ::: declare the major version for your API.
            // This number must correspond to your URL version ( eg. '/myservice/v1/')
            // format: ## = Integer
            version = "1",

            // By default the URL version will be defined in @ApplicationPath('/myservice/v1') annotation
            // see UriPatternEnum class for alternatives
            uriPattern = UriPatternEnum.VERSION_IN_RESOURCE,

            // Human Readable description of Api.
            // This long explanation will be presented in external documentation.
            description = "TODO:Fill in detailed explanation of this api",

            // link to internal documentation to aid COPR review
            documentationLink = "TODO:Provide Link to Internal Documentation"

        )
    }
)
public interface MKTTrackService {
}