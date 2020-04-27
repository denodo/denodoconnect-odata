package com.denodo.connect.odata.wrapper.util;

import com.denodo.vdb.engine.customwrapper.CustomWrapperSchemaParameter;

public class SchemaParameterUtils {

    public static int getSchemaParameterType(final String nameParam, final CustomWrapperSchemaParameter[] schemaParameters) {

        for (final CustomWrapperSchemaParameter param : schemaParameters) {
            if (nameParam.equalsIgnoreCase(param.getName())) {
                return param.getType();
            }
        }

        return -1;
    }

    public static String getSchemaParameterName(final String nameParam, final CustomWrapperSchemaParameter[] schemaParameters) {

        for (final CustomWrapperSchemaParameter param : schemaParameters) {

            if (nameParam.equalsIgnoreCase(param.getName())) {
                return param.getName();
            }
        }

        return null;
    }

    public static CustomWrapperSchemaParameter[] getSchemaParameterColumns(final String nameParam,
        final CustomWrapperSchemaParameter[] schemaParameters) {

        for (final CustomWrapperSchemaParameter param : schemaParameters) {

            if (nameParam.equalsIgnoreCase(param.getName())) {
                return param.getColumns();
            }
        }

        return null;
    }

}
