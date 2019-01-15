![AirMap: The Airspace Platform for Developers](AirMap.png)
[![Bintray](https://img.shields.io/bintray/v/airmapio/maven/com.airmap.airmapsdk.svg)](http://jcenter.bintray.com/com/airmap/airmapsdk/airmapsdk/)
[![license](https://img.shields.io/github/license/airmap/AirMapSDK-Android.svg)](https://github.com/airmap/AirMapSDK-Android/blob/master/LICENSE)

Create Flights, Send Telemetry Data, Get Realtime Traffic Alerts.

## Requirements
* Minimum Android SDK Level 18 or higher

### Sign up for an [AirMap Developer Account.](https://dashboard.airmap.io/developer/)

 [https://dashboard.airmap.io/developer](https://dashboard.airmap.io/developer)
 
 
### Read Getting Started Guide
[https://developers.airmap.com/v2.1/docs/getting-started-with-airmap]()

## Setup

Start by adding the Android SDK to your project:

* Add 
```groovy
implementation 'com.airmap.airmapsdk:airmapsdk:2.0.1'https://developers.airmap.com/docs/getting-started-android
``` 

to the dependencies section of your module level `build.gradle` file

* Add
```groovy
manifestPlaceholders = [auth0Domain: "@string/com_auth0_domain", auth0Scheme: "@string/com_auth0_scheme"]
```

to the defaultConfig section of your module level `build.gradle` file

* Add 
```groovy
maven { url "https://jitpack.io" }
``` 

to your application-level `build.gradle` file under the `allprojects.repositories` block

### Initalizing The SDK

Simply add this line in your Application or Activity's `onCreate`

```java
AirMap.init(this);
```

### Documentation
Visit [https://developers.airmap.com/](https://developers.airmap.com/) for the full documentation

# License
See [LICENSE](https://raw.githubusercontent.com/airmap/AirMapSDK-Android/master/LICENSE) for details.
