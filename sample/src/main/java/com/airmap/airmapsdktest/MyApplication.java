package com.airmap.airmapsdktest;

import android.app.Application;

import com.airmap.airmapsdk.networking.services.AirMap;
import com.mapbox.mapboxsdk.MapboxAccountManager;

import static com.airmap.airmapsdk.util.Utils.getMapboxApiKey;

/**
 * Created by Vansh Gandhi on 8/12/16.
 * Copyright © 2016 AirMap, Inc. All rights reserved.
 */
public class MyApplication extends Application {
    @Override
    public void onCreate() {
        super.onCreate();
        AirMap.init(this);
        AirMap.enableLogging(true);
        MapboxAccountManager.start(this, getMapboxApiKey());
    }
}
