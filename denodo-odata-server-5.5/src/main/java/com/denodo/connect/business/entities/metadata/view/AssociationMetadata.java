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
package com.denodo.connect.business.entities.metadata.view;

import org.apache.olingo.odata2.api.edm.EdmMultiplicity;

public class AssociationMetadata {

	private String AssociationName;
	private String leftRole;
	private String leftViewName;
	private EdmMultiplicity leftMultiplicity;
	private String rightRole;
	private String rightViewName;
	private EdmMultiplicity rightMultiplicity;
	private String mappings;
	private String asocciationDescription;
	private Boolean valid;
	
	
	public AssociationMetadata() {
		
	}


	public String getLeftRole() {
		return leftRole;
	}


	public void setLeftRole(String leftRole) {
		this.leftRole = leftRole;
	}


	public String getLeftViewName() {
		return leftViewName;
	}


	public void setLeftViewName(String leftViewName) {
		this.leftViewName = leftViewName;
	}


	
	public EdmMultiplicity getLeftMultiplicity() {
		return leftMultiplicity;
	}


	public void setLeftMultiplicity(EdmMultiplicity leftMultiplicity) {
		this.leftMultiplicity = leftMultiplicity;
	}


	public EdmMultiplicity getRightMultiplicity() {
		return rightMultiplicity;
	}


	public void setRightMultiplicity(EdmMultiplicity rightMultiplicity) {
		this.rightMultiplicity = rightMultiplicity;
	}


	public String getRightRole() {
		return rightRole;
	}


	public void setRightRole(String rightRole) {
		this.rightRole = rightRole;
	}


	public String getRightViewName() {
		return rightViewName;
	}


	public void setRightViewName(String rightViewName) {
		this.rightViewName = rightViewName;
	}


	public String getMappings() {
		return mappings;
	}


	public void setMappings(String mappings) {
		this.mappings = mappings;
	}


	public String getAsocciationDescription() {
		return asocciationDescription;
	}


	public void setAsocciationDescription(String asocciationDescription) {
		this.asocciationDescription = asocciationDescription;
	}


	public Boolean getValid() {
		return valid;
	}


	public void setValid(Boolean valid) {
		this.valid = valid;
	}


	public String getAssociationName() {
		return AssociationName;
	}


	public void setAssociationName(String associationName) {
		AssociationName = associationName;
	}


	@Override
	public String toString() {
		return "AssociationMetadata [AssociationName=" + AssociationName
				+ ", leftRole=" + leftRole + ", leftViewName=" + leftViewName
				+ ", leftMultiplicity=" + leftMultiplicity.toString() + ", rightRole="
				+ rightRole + ", rightViewName=" + rightViewName
				+ ", rightMultiplicity=" + rightMultiplicity.toString() + ", mappings="
				+ mappings + ", asocciationDescription="
				+ asocciationDescription + ", valid=" + valid + "]";
	}




	

	
	
}
