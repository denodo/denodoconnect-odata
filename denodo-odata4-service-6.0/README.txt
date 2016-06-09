=============================================================================
DENODO ODATA SERVICE
=============================================================================

Denodo OData Service allows users to connect to the Denodo Platform and query
its databases using an OData 4.0 interface.

The project is distributed as a binary war file (Java Web application ARchive)

In order to run the Denodo OData Service you need to deploy this war file in
a Java web application container. Apache Tomcat 7 is recommended.

By default, the Denodo OData Service will try to connect with a local Denodo
Platform installation listening on localhost:9999. This can be changed at the
configuration.properties file in the /WEB-INF/classes folder inside the
distributed .war file once deployed and unarchived, or by manually accessing
it inside the archive (.war files are .zip files).

URLs are of the form:

  http://<SERVER>:<PORT>/<WEBAPP_CONTEXT>/denodo-odata.svc/<DATABASE_NAME>

Note that the value for <WEBAPP_CONTEXT> will depend on the way the .war is
deployed into the application server. Apache Tomcat will create by default a
context called "denodo-odata4-service-6.0" for a .war file called
"denodo-odata4-service-6.0.war".

For providing VDP credentials, HTTP Basic Auth is used. Credentials are passed
through to the underlying VDP server for authentication.

REST API Testing tools like Postman (https://www.getpostman.com/) or
Paw (https://luckymarmot.com/paw) are recommended for testing OData services.
Note there is no HTML interface in this application.

For more info, see the User Manual at the /doc folder.

This software is part of the DenodoConnect component collection.

Copyright (c) 2016, denodo technologies (http://www.denodo.com)
