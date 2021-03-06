package org.z.distributer;

public class ClientState {
    private double maxLongitude;
    private double minLongitude;
    private double maxLatitude;
    private double minLatitude;

    private double centerLongitude;
    private double centerLatitude;
    private double queryRadius;

    public ClientState(double maxLongitude, double minLangitude, double maxLatitude, double minLatitude) {
        this.maxLongitude = maxLongitude;
        this.minLongitude = minLangitude;
        this.maxLatitude = maxLatitude;
        this.minLatitude = minLatitude;

        this.centerLongitude = (maxLongitude + minLangitude) / 2;
        this.centerLatitude = (maxLatitude + minLatitude) / 2;
        this.queryRadius = computeQueryRadius();
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
}
