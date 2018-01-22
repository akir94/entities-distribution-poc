package org.z.distributer.common;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClientState {
    private double maxLongitude;
    private double minLongitude;
    private double maxLatitude;
    private double minLatitude;

    private double centerLongitude;
    private double centerLatitude;
    private double queryRadius;

    private Map<String, Instant> previousRedisTimes;

    public ClientState(double maxLongitude, double minLangitude, double maxLatitude, double minLatitude) {
        this.maxLongitude = maxLongitude;
        this.minLongitude = minLangitude;
        this.maxLatitude = maxLatitude;
        this.minLatitude = minLatitude;

        this.centerLongitude = (maxLongitude + minLangitude) / 2;
        this.centerLatitude = (maxLatitude + minLatitude) / 2;
        this.queryRadius = computeQueryRadius();

        this.previousRedisTimes = new ConcurrentHashMap<>();
    }

    private double computeQueryRadius() {
        double x = maxLongitude - centerLongitude;
        double y = maxLatitude - centerLatitude;
        double radiusInDegrees = Math.sqrt(x*x + y*y);
        double radiusInNauticalMiles = radiusInDegrees * 60.0;
        double radiusInKilometers = radiusInNauticalMiles * 1.851999326;
        return radiusInKilometers;
    }

    public double getCenterLongitude() {
        return centerLongitude;
    }

    public double getCenterLatitude() {
        return centerLatitude;
    }

    public double getQueryRadius() {
        return queryRadius;
    }

    public Map<String, Instant> getPreviousRedisTimes() {
        return previousRedisTimes;
    }

    public boolean isInBounds(double longitude, double latitude) {
        return maxLongitude > longitude && longitude > minLongitude &&
                maxLatitude > latitude && latitude > minLatitude;
    }

    public void setPreviousRedisTimes(Map<String, Instant> previousRedisTimes) {
        this.previousRedisTimes = previousRedisTimes;
    }
}
