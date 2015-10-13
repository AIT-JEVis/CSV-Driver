/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jevis.csvparser;

/**
 *
 * @author broder
 */
public class DataPoint {
    
    private String mappingIdentifier;
    private Integer valueIndex;
    private Long target;

    public String getMappingIdentifier() {
        return mappingIdentifier;
    }

    public void setMappingIdentifier(String mappingIdentifier) {
        this.mappingIdentifier = mappingIdentifier;
    }

    public Integer getValueIndex() {
        return valueIndex;
    }

    public void setValueIndex(Integer valueIndex) {
        this.valueIndex = valueIndex;
    }

    public Long getTarget() {
        return target;
    }

    public void setTarget(Long target) {
        this.target = target;
    }


    
}
