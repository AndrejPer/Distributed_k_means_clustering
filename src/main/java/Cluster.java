import java.awt.*;
import java.util.ArrayList;

public class Cluster {
    private Site centroid;
    private ArrayList<Site> sites;
    private final int clusterID;
    private final Color color;


    public Cluster(Site initial, Color color, int id) {
        this.centroid = new Site(initial.getLatitude(), initial.getLongitude(), 0, -1);
        this.sites = new ArrayList<>();
        this.clusterID = id;
        this.color = color;

    }

    public Site getCentroid() {
        return centroid;
    }

    public ArrayList<Site> getSites() {
        return sites;
    }

    public int getClusterID() {
        return clusterID;
    }

    public Color getColor() {
        return color;
    }

    public double getWeight() { return this.centroid.getWeight();}

    public void setWeight(double weight) {
        this.centroid.setWeight(weight);
    }

    public void setSites(ArrayList<Site> sites) {this.sites = sites;}

    public synchronized void addSite(Site site) {
            this.sites.add(site);
    }

    //weighted mean calculation for updating the centroid of cluster
    public boolean updateCentroid() {
        Site initialCentroid = new Site(this.getCentroid().getLatitude(), this.centroid.getLongitude(), this.centroid.getWeight(), this.centroid.getSiteID());
        if(sites.size() == 0) return false;

        double sumX = 0, sumY = 0, weight = 0;
        for (Site site: sites) {
                sumX += site.getLatitude() * site.getWeight();
                sumY += site.getLongitude() * site.getWeight();
                weight += site.getWeight();
        }
        this.centroid = new Site(sumX / weight, sumY / weight, weight, -1);

        if(initialCentroid.getLatitude() != this.centroid.getLatitude() || initialCentroid.getLongitude() != this.centroid.getLongitude()) return true;
        return false;
    }

}
