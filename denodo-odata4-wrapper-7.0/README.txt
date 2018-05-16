
 KNOWN LIMITATIONS
===================

This custom wrapper has the following known limitations:

	-This custom data source currently only works with OData version 4.0. The previous versions of Odata are not supported by this wrapper. There is other custom wrapper to access to old versions of OData. 
 
	-Filtering of elements by array properties is not supported. OData does not allow this kind of searches.

	-Filtering by media read links properties is not supported.

	-The insertion of arrays is not supported.

	-The authentication using NTLM through a proxy is not supported.

	-The insertion or update of  media file properties is not supported.

	-Addressing derived types properties is not available. When you have entities with a type derived from the declared type of the requested collection, their properties will be added to the schema of the base view but you can't project these properties separately.



 OTHER PROPERTIES
===================

Other configurable properties are stored into the file “customwrapper.properties”. 

If the OData specification changes in the future, you can change it easily:
 - timeformat : is the format used by OData to represents a date.
 - namespace  : is the default namespace used by OData.