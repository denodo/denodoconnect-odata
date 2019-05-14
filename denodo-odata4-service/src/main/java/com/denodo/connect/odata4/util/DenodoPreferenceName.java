/*
 *
 * Copyright (c) 2019. DENODO Technologies.
 * http://www.denodo.com
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of DENODO
 * Technologies ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with DENODO.
 *
 */
package com.denodo.connect.odata4.util;

public enum DenodoPreferenceName {

    ENTITY_STREAMING("odata.entitystreaming");

    private final String preferenceName;

    DenodoPreferenceName(final String preferenceName) {
        this.preferenceName = preferenceName;
    }

    public String getName() {
        return preferenceName;
    }

    @Override
    public String toString() {
        return getName();
    }
}