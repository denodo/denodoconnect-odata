/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2014-2015, denodo technologies (http://www.denodo.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * =============================================================================
 */
package com.denodo.connect;

import java.net.URI;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.denodo.connect.business.services.entity.EntityService;
import org.apache.log4j.Logger;
import org.apache.olingo.odata2.api.edm.EdmEntitySet;
import org.apache.olingo.odata2.api.edm.EdmLiteralKind;
import org.apache.olingo.odata2.api.edm.EdmProperty;
import org.apache.olingo.odata2.api.edm.EdmSimpleType;
import org.apache.olingo.odata2.api.ep.EntityProvider;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties;
import org.apache.olingo.odata2.api.ep.EntityProviderWriteProperties.ODataEntityProviderPropertiesBuilder;
import org.apache.olingo.odata2.api.exception.ODataException;
import org.apache.olingo.odata2.api.exception.ODataNotFoundException;
import org.apache.olingo.odata2.api.exception.ODataNotImplementedException;
import org.apache.olingo.odata2.api.processor.ODataResponse;
import org.apache.olingo.odata2.api.processor.ODataSingleProcessor;
import org.apache.olingo.odata2.api.processor.part.EntitySetProcessor;
import org.apache.olingo.odata2.api.processor.part.EntitySimplePropertyProcessor;
import org.apache.olingo.odata2.api.processor.part.EntitySimplePropertyValueProcessor;
import org.apache.olingo.odata2.api.uri.ExpandSelectTreeNode;
import org.apache.olingo.odata2.api.uri.KeyPredicate;
import org.apache.olingo.odata2.api.uri.NavigationSegment;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetCountUriInfo;
import org.apache.olingo.odata2.api.uri.UriParser;
import org.apache.olingo.odata2.api.uri.info.GetEntitySetUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetEntityUriInfo;
import org.apache.olingo.odata2.api.uri.info.GetSimplePropertyUriInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DenodoDataSingleProcessor extends ODataSingleProcessor {

    private static final Logger logger = Logger.getLogger(DenodoDataSingleProcessor.class);

    @Autowired
    private EntityService entityService;

    @Override
    public ODataResponse readEntitySet(final GetEntitySetUriInfo uriInfo, final String contentType) throws ODataException {

        EdmEntitySet entitySet;

        if (uriInfo.getNavigationSegments().size() == 0) {
            entitySet = uriInfo.getStartEntitySet();

            try {
                List<Map<String, Object>> data = this.entityService.getEntitySet(entitySet.getName(), uriInfo);
                if (data != null && !data.isEmpty()) {
                    URI serviceRoot = getContext().getPathInfo().getServiceRoot();
                    ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(serviceRoot);

                    // Transform the list of selected properties into an
                    // expand/select tree
                    ExpandSelectTreeNode expandSelectTreeNode = UriParser.createExpandSelectTree(uriInfo.getSelect(), uriInfo.getExpand());
                    propertiesBuilder.expandSelectTree(expandSelectTreeNode);
                    // propertiesBuilder.contentOnly(true);

                    return EntityProvider.writeFeed(contentType, entitySet, data, propertiesBuilder.build());
                }
            } catch (SQLException e) {
                logger.error(e);
            }

            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);

        } else if (uriInfo.getNavigationSegments().size() == 1) {
            // I think that this case is for relationships
            // navigation first level, simplified example for illustration
            // purposes only
            Map<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();

            EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            try {
                List<Map<String, Object>> data = this.entityService.getEntitySetAssociation(entitySetStart.getName(), keys,
                        navigationSegments, entitySetTarget.getName());
                if (data != null && !data.isEmpty()) {
                    return EntityProvider.writeFeed(contentType, entitySetTarget, data,
                            EntityProviderWriteProperties.serviceRoot(getContext().getPathInfo().getServiceRoot()).build());
                }
            } catch (SQLException e) {
                logger.error(e);
            }

            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
        }

        throw new ODataNotImplementedException();
    }

    @Override
    public ODataResponse readEntity(final GetEntityUriInfo uriInfo, final String contentType) throws ODataException {

        if (uriInfo.getNavigationSegments().size() == 0) {
            EdmEntitySet entitySet = uriInfo.getStartEntitySet();

            Map<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());

            try {
                Map<String, Object> data = this.entityService.getEntity(entitySet.getName(), keys, uriInfo);
                if (data != null && !data.isEmpty()) {
                    URI serviceRoot = getContext().getPathInfo().getServiceRoot();
                    ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(serviceRoot);

                    // Transform the list of selected properties into an
                    // expand/select tree
                    ExpandSelectTreeNode expandSelectTreeNode = UriParser.createExpandSelectTree(uriInfo.getSelect(), uriInfo.getExpand());
                    propertiesBuilder.expandSelectTree(expandSelectTreeNode);
                    //propertiesBuilder.contentOnly(true);

                    return EntityProvider.writeEntry(contentType, entitySet, data, propertiesBuilder.build());
                }
            } catch (SQLException e) {
                logger.error(e);
            }

            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);

        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            // I think that this case is for relationships
            // navigation first level, simplified example for illustration
            // purposes only
            Map<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();
            EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            try {
                Map<String, Object> data = this.entityService.getEntityAssociation(entitySetStart.getName(), keys, navigationSegments,
                        entitySetTarget.getName());
                if (data != null && !data.isEmpty()) {
                    URI serviceRoot = getContext().getPathInfo().getServiceRoot();
                    ODataEntityProviderPropertiesBuilder propertiesBuilder = EntityProviderWriteProperties.serviceRoot(serviceRoot);

                    return EntityProvider.writeEntry(contentType, entitySetTarget, data, propertiesBuilder.build());
                }
            } catch (SQLException e) {
                logger.error(e);
            }

            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
        }

        throw new ODataNotImplementedException();
    }

    // private int getKeyValue(final KeyPredicate key) throws ODataException {
    // EdmProperty property = key.getProperty();
    // EdmSimpleType type = (EdmSimpleType) property.getType();
    // return type.valueOfString(key.getLiteral(), EdmLiteralKind.DEFAULT,
    // property.getFacets(), Integer.class);
    // }

    private static Map<String, Object> getKeyValues(final List<KeyPredicate> keyList) throws ODataException {
        Map<String, Object> keys = new HashMap<String, Object>();
        for (KeyPredicate key : keyList) {
            EdmProperty property = key.getProperty();
            EdmSimpleType type = (EdmSimpleType) property.getType();
            Object value = type.valueOfString(key.getLiteral(), EdmLiteralKind.DEFAULT, property.getFacets(), Object.class);
            keys.put(property.getName(), value);
        }
        return keys;
    }

    /**
     * @see EntitySimplePropertyValueProcessor
     */
    @Override
    public ODataResponse readEntitySimplePropertyValue(final GetSimplePropertyUriInfo uriInfo, final String contentType)
            throws ODataException {
        if (uriInfo.getNavigationSegments().size() == 0) {
            EdmEntitySet entitySet = uriInfo.getStartEntitySet();

            EdmProperty property = uriInfo.getPropertyPath().get(0);
            Map<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            try {
                Map<String, Object> data = this.entityService.getEntity(entitySet.getName(), keys, property);
                if (data != null && !data.isEmpty()) {

                    return EntityProvider.writePropertyValue(property, data.get(property.getName()));
                }
            } catch (SQLException e) {
                logger.error(e);
            }

            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);

        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            // I think that this case is for relationships
            // navigation first level, simplified example for illustration
            // purposes only
            List<EdmProperty> properties = uriInfo.getPropertyPath();
            Map<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();
            EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            try {
                Map<String, Object> data = this.entityService.getEntityAssociation(entitySetStart.getName(), keys, navigationSegments,
                        entitySetTarget.getName());
                if (data != null && !data.isEmpty()) {
                    return EntityProvider.writePropertyValue(properties.get(0), data.get(properties.get(0).getName()));
                }
            } catch (SQLException e) {
                logger.error(e);
            }

            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
        }

        throw new ODataNotImplementedException();
    }

    /**
     * @see EntitySimplePropertyProcessor
     */
    @Override
    public ODataResponse readEntitySimpleProperty(final GetSimplePropertyUriInfo uriInfo, final String contentType) throws ODataException {

        if (uriInfo.getNavigationSegments().size() == 0) {
            EdmEntitySet entitySet = uriInfo.getStartEntitySet();

            EdmProperty property = uriInfo.getPropertyPath().get(0);
            Map<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            try {
                Map<String, Object> data = this.entityService.getEntity(entitySet.getName(), keys, property);
                if (data != null && !data.isEmpty()) {

                    return EntityProvider.writeProperty(contentType, property, data.get(property.getName()));
                }
            } catch (SQLException e) {
                logger.error(e);
            }

            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);

        } else if (uriInfo.getNavigationSegments().size() >= 1) {
            // I think that this case is for relationships
            // navigation first level, simplified example for illustration
            // purposes only
            EdmProperty property = uriInfo.getPropertyPath().get(0);
            Map<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
            List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();
            EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
            EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();

            try {
                Map<String, Object> data = this.entityService.getEntityAssociation(entitySetStart.getName(), keys, navigationSegments,
                        entitySetTarget.getName(), property);
                if (data != null && !data.isEmpty()) {
                    return EntityProvider.writeProperty(contentType, property, data.get(property.getName()));
                }
            } catch (SQLException e) {
                logger.error(e);
            }

            throw new ODataNotFoundException(ODataNotFoundException.ENTITY);
        }

        throw new ODataNotImplementedException();
    }
	

	  /**
	   * @see EntitySetProcessor
	   */
	  @Override
	  public ODataResponse countEntitySet(final GetEntitySetCountUriInfo uriInfo, final String contentType)
	          throws ODataException {
	      EdmEntitySet entitySet;

	      //TODO
	      if (uriInfo.getNavigationSegments().size() == 0) {
	          entitySet = uriInfo.getStartEntitySet();
	          try {
	              Integer count = this.entityService.getCount(entitySet.getName(), uriInfo);
	              if (count != null ) {
	                  return EntityProvider.writeText(count.toString());
	              }
	          } catch (SQLException e) {
	              logger.error(e);
	          }

	          throw new ODataNotFoundException(ODataNotFoundException.ENTITY);

	      } else if (uriInfo.getNavigationSegments().size() == 1) {
	          // I think that this case is for relationships
	          // navigation first level, simplified example for illustration
	          // purposes only
	          Map<String, Object> keys = getKeyValues(uriInfo.getKeyPredicates());
	          List<NavigationSegment> navigationSegments = uriInfo.getNavigationSegments();

	          EdmEntitySet entitySetTarget = uriInfo.getTargetEntitySet();
	          EdmEntitySet entitySetStart = uriInfo.getStartEntitySet();


	          try {
	              Integer count = this.entityService.getCountAssociation(entitySetStart.getName(), keys, navigationSegments, entitySetTarget.getName());
	              if (count != null ) {
	                  return EntityProvider.writeText(count.toString());
	              }
	          } catch (SQLException e) {
	              logger.error(e);
	          }

	          throw new ODataNotFoundException(ODataNotFoundException.ENTITY);

	      }
	      throw new ODataNotImplementedException();
	  }
}
