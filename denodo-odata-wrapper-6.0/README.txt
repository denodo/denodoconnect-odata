
 KNOWN LIMITATIONS
===================

This custom wrapper has the following known limitations:

 - Only works with versions 1.0 or 2.0 of oData. oData version 3.0 is partially support interpreting it as a lower version. May not work.
  
   More information:   http://code.google.com/p/odata4j/wiki/Roadmap
                       https://groups.google.com/forum/#!topic/odata4j-discuss/ozUuCASqGL8
 
 - You can't filter elements specified obtained through "expand" items. VDP must filter these items using ROW sintax in the query.
   
   More information:   http://msdn.microsoft.com/en-us/library/fp142385%28v=office.15%29.aspx


 OTHER PROPERTIES
===================

Other configurable properties are stored into the file “customwrapper.properties”. 

If the OData specification changes in the future, you can change it easily:
 - timeformat : is the format used by OData to represents a date.
 - namespace  : is the default namespace used by OData.