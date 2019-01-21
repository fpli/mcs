package com.ebay.app.raptor.chocolate;

import com.ebay.cos.raptor.service.annotations.Api;
import com.ebay.cos.raptor.service.annotations.Service;
import com.ebay.cos.raptor.service.annotations.UriPatternEnum;

/**
 * This @Service annotated interface is used by the SDD Generator to automatically produce documentation
 * which describes your service and the Apis which it supports.
 *
 * ADDITIONAL LINKS:
 *
 *  https://github.corp.ebay.com/commerceos/standards-annotation-component/blob/master/service-descriptor-annotations.md
 *
 *  https://github.corp.ebay.com/commerceos/cos-naming-conventions/blob/master/naming-examples.md
 *  https://github.corp.ebay.com/commerceos/cos-idm/blob/master/idm.md
 *
 */
@Service(

	// Human Readable project name.
	// Used with Version to distinctly identify your project in Fulcrum.
	// format: Letters, Numbers and Spaces
	name="TODO:FillInName",

	// Service Version as reported in Fulcrum.
	// This is *not* the same as your pom/version.
	// format:  ##.##.## = Major.Minor.Maintenance
	version="1.0.0",

	// Human Readable description of Service.
	// This long explanation will be presented in external documentation.
	description="TODO:Fill in detailed explanation of this service",

	api = {

		// First Api supported by this Service.
		// When you create additional APIs, or add versions to an API, create additional entries here.
		@Api(

			// Context and Name of this service organized as either ( 'context' or 'context/name' )
			// The context and name for your service will be finalized during COPR review.
			// format:  Lowercase letters and numbers, single words ( no special characters )
			name="samplesvc", // TODO: fill in 'context' or 'context/name'

			// title ::: short, human readable description of the service
			title="TODO:Fill In Api Title",

			// version ::: declare the major version for your API.
			// This number must correspond to your URL version ( eg. '/myservice/v1/')
			// format: ## = Integer
			version = "1",

			// By default the URL version will be defined in @ApplicationPath('/myservice/v1') annotation
			// see UriPatternEnum class for alternatives
			uriPattern = UriPatternEnum.VERSION_IN_RESOURCE,

			// Human Readable description of Api.
			// This long explanation will be presented in external documentation.
			description="TODO:Fill in detailed explanation of this api",

			// link to internal documentation to aid COPR review
			documentationLink="TODO:Provide Link to Internal Documentation"

		)
	}
)
public interface SampleService {}