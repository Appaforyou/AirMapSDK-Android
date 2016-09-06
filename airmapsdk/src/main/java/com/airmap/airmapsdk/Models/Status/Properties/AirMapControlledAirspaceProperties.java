package com.airmap.airmapsdk.models.status.properties;

import com.airmap.airmapsdk.models.AirMapBaseModel;

import org.json.JSONObject;

import java.io.Serializable;

/**
 * Created by Vansh Gandhi on 7/15/16.
 * Copyright © 2016 AirMap, Inc. All rights reserved.
 */
@SuppressWarnings("unused")
public class AirMapControlledAirspaceProperties implements Serializable, AirMapBaseModel {
    private String classAirspace;
    private String airportIdentifier;

    /**
     * Initialize an AirMapControlledAirspaceProperties from JSON
     *
     * @param propertiesJson The JSON representation
     */
    public AirMapControlledAirspaceProperties(JSONObject propertiesJson) {
        constructFromJson(propertiesJson);
    }

    public AirMapControlledAirspaceProperties() {

    }

    @Override
    public AirMapControlledAirspaceProperties constructFromJson(JSONObject json) {
        if (json != null) {
            setClassAirspace(json.optString("classAirspace"));
            setAirportIdentifier(json.optString("airportIdentifier"));
        }
        return this;
    }

    public String getClassAirspace() {
        return classAirspace;
    }

    public AirMapControlledAirspaceProperties setClassAirspace(String classAirspace) {
        this.classAirspace = classAirspace;
        return this;
    }

    public String getAirportIdentifier() {
        return airportIdentifier;
    }

    public AirMapControlledAirspaceProperties setAirportIdentifier(String airportIdentifier) {
        this.airportIdentifier = airportIdentifier;
        return this;
    }
}
