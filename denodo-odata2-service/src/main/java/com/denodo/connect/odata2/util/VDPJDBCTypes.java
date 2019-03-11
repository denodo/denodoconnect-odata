/*
 * =============================================================================
 *
 *   This software is part of the DenodoConnect component collection.
 *
 *   Copyright (c) 2018, Denodo Technologies (http://www.denodo.com)
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
package com.denodo.connect.odata2.util;

/**
 * Class with constants from com.denodo.vdb.vdbinterface.common.clientResult.vo.descriptions.type.util.JDBCTypeUtil.
 * Emulates java.sql.Types for interval types since VDP 7.0.
 *
 */
public final class VDPJDBCTypes {

    
        public static final int TIMESTAMP_WITH_TIMEZONE = 2014;
        public static final int INTERVAL_YEAR_MONTH = 2020;
        public static final int INTERVAL_DAY_SECOND = 2021;
        
        private VDPJDBCTypes() {
            
        }


}
