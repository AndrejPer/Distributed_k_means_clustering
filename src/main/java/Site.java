
public class Site {
    private double latitude,
            longitude;
    private double weight;
    private int clusterID;
    private final int siteID;

    public Site(double latitude, double longitude, double w, int siteID) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.weight = w;
        this.siteID = siteID;
    }


    public double getWeight() {
        return weight;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public int getClusterID() {
        return clusterID;
    }

    public int getSiteID() {return siteID;}

    public void setClusterID(int clusterID) {
        this.clusterID = clusterID;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public void setLongitude(double lon) {
        this.longitude = lon;
    }

    public void setWeight(double w) { this.weight = w;}

    public String toString() {
        return "(" + getLatitude() + ", " + getLongitude() + ")";
    }

}
