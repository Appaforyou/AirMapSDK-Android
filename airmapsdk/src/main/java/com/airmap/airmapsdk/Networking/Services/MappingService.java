package com.airmap.airmapsdk.Networking.Services;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Vansh Gandhi on 7/7/16.
 * Copyright © 2016 AirMap, Inc. All rights reserved.
 */
@SuppressWarnings("unused")
public class MappingService extends BaseService {

    public enum AirMapLayerType implements Serializable {
        TFRS("tfrs", "Temporary Flight Restrictions", "Temporary flight restriction"),
        Wildfires("wildfires", "Wildfires", "Wildfires"),
        Prohibited("sua_prohibited", "Prohibited Area", "Prohibited airspace"),
        Restricted("sua_restricted", "Restricted Area", "Restricted airspace"),
        NationalParks("national_parks", "National Park", "National park"),
        NOAA("noaa", "NOAA", "NOAA marine protection area"),
        Schools("schools", "School", "School"),
        Hospitals("hospitals", "Hospital", "Hospital"),
        Heliports("heliports", "Heliport", "Heliport"),
        PowerPlants("power_plants", "Power Plant", "Power plant"),
        AirportsCommercial("airports_commercial", "Airport", "Airport"),
        AirportsRecreational("airports_recreational", "Airport", "Airport"),
        AirportsCommercialPrivate("airports_commercial_private", "Private Airport", "Private airport"),
        AirportsRecreationalPrivate("airports_recreational_private", "Private Airport", "Private airport"),
        ClassB("class_b", "Class B Airspace", "Class B controlled airspace"),
        ClassC("class_c", "Class C Airspace", "Class C controlled airspace"),
        ClassD("class_d", "Class D Airspace", "Class D controlled airspace"),
        ClassE("class_e0", "Class E Airspace", "Class E controlled airspace to the surface"),
        EssentialAirspace("class_b,class_c,class_d,class_e0", "Essential Airspace (B, C, D & E)", "Essential airspace (B, C, D & E)");

        private final String text;
        private final String title;
        private final String description;

        AirMapLayerType(String text, String title, String description) {
            this.text = text;
            this.title = title;
            this.description = description;
        }

        @Override
        public String toString() {
            return text;
        }

        public String getTitle() {
            return title;
        }

        public String getDescription() {
            return description;
        }

        public static AirMapLayerType fromString(String text) {
            switch (text) {
                case "tfrs":
                    return TFRS;
                case "wildfires":
                    return Wildfires;
                case "sua_prohibited":
                    return Prohibited;
                case "sua_restricted":
                    return Restricted;
                case "national_parks":
                    return NationalParks;
                case "noaa":
                    return NOAA;
                case "schools":
                    return Schools;
                case "hospitals":
                    return Hospitals;
                case "heliports":
                    return Heliports;
                case "power_plants":
                    return PowerPlants;
                case "airports_commercial":
                    return AirportsCommercial;
                case "airports_recreational":
                    return AirportsRecreational;
                case "airports_commercial_private":
                    return AirportsCommercialPrivate;
                case "airports_recreational_private":
                    return AirportsRecreationalPrivate;
                case "class_b":
                    return ClassB;
                case "class_c":
                    return ClassC;
                case "class_d":
                    return ClassD;
                case "class_e0":
                    return ClassE;
                case "class_b,class_c,class_d,class_e0":
                    return EssentialAirspace;
                default:
                    return null;
            }
        }

        public AirMapAirspaceType getAirspaceType() {
            switch (this) {
                case TFRS:
                    return AirMapAirspaceType.TFR;
                case Wildfires:
                    return AirMapAirspaceType.Wildfires;
                case Prohibited:
                case Restricted:
                    return AirMapAirspaceType.SpecialUse;
                case NationalParks:
                case NOAA:
                    return AirMapAirspaceType.Park;
                case Schools:
                    return AirMapAirspaceType.School;
                case Hospitals:
                    return AirMapAirspaceType.Hospitals;
                case PowerPlants:
                    return AirMapAirspaceType.PowerPlant;
                case AirportsCommercial:
                case AirportsRecreational:
                case AirportsCommercialPrivate:
                case AirportsRecreationalPrivate:
                    return AirMapAirspaceType.Airport;
                case Heliports:
                case ClassB:
                case ClassC:
                case ClassD:
                case ClassE:
                case EssentialAirspace:
                    return AirMapAirspaceType.ControlledAirspace;
            }
            return null;
        }
    }

    public enum AirMapAirspaceType {
        Airport("airport", "Airport"),
        Heliport("heliport", "Heliport"),
        Park("park", "Park"),
        PowerPlant("power_plant", "Power Plant"),
        ControlledAirspace("controlled_airspace", "Controlled Airspace"),
        School("school", "School"),
        SpecialUse("special_use_airspace", "Special Use Airspace"),
        TFR("tfr", "TFR"),
        Wildfires("wildfire", "Wildfire"),
        Hospitals("hospital", "Hospital");

        private final String text;
        private final String title;

        AirMapAirspaceType(String text, String title) {
            this.text = text;
            this.title = title;
        }

        @Override
        public String toString() {
            return text;
        }

        public String getTitle() {
            return title;
        }

        public static AirMapAirspaceType fromString(String text) {
            switch (text) {
                case "airport":
                    return Airport;
                case "heliport":
                    return Heliport;
                case "park":
                    return Park;
                case "power_plant":
                    return PowerPlant;
                case "controlled_airspace":
                    return ControlledAirspace;
                case "school":
                    return School;
                case "special_use_airspace":
                    return SpecialUse;
                case "tfr":
                    return TFR;
                case "wildfire":
                    return Wildfires;
                case "hospital":
                    return Hospitals;
            }
            return null;
        }
    }

    public enum AirMapMapTheme {
        Standard("standard"),
        Light("light"),
        Dark("dark"),
        Satellite("satellite");

        private final String text;

        AirMapMapTheme(String text) {
            this.text = text;
        }

        @Override
        public String toString() {
            return text;
        }

        public static AirMapMapTheme fromString(String text) {
            switch (text) {
                case "standard":
                    return Standard;
                case "light":
                    return Light;
                case "dark":
                    return Dark;
                case "satellite":
                    return Satellite;
            }
            return null;
        }
    }

    protected String getTileSourceUrl(List<AirMapLayerType> layers, AirMapMapTheme theme) {
        String tiles = layers.size() == 0 ? "_-_" : android.text.TextUtils.join(",", layers);
        return mapTilesBaseUrl + tiles + "?&theme=" + theme.toString() + "&apikey=" + AirMap.getInstance().getApiKey() + "&token=" + AirMap.getInstance().getApiKey();
    }
}
