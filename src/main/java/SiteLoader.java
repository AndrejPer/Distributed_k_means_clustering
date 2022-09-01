import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;
import java.util.Random;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;
import static java.lang.Double.parseDouble;


public class SiteLoader {
    //private int siteCount;

    private static final SiteLoader instance = new SiteLoader();

    public SiteLoader() {
    }

    static SiteLoader getInstance() {return instance;}

    public ArrayList<Site> loadSites(int siteCount) {
        ArrayList<Site> sites = new ArrayList<Site>();
        JSONParser parser = new JSONParser();
        int siteID = 0;

        try{
            String file = Objects.requireNonNull(SiteLoader.class.getClassLoader().getResource("sites.json")).getFile();
            JSONArray jsonArray = (JSONArray) parser.parse(new FileReader(file));
            Random random = new Random();

            double minLat = Double.MAX_VALUE, maxLat = Double.MIN_VALUE,
                    minLng = Double.MAX_VALUE, maxLng = Double.MIN_VALUE,
                    maxCapacity = Double.MIN_VALUE;
            Iterator<JSONObject> iterator = jsonArray.iterator();

            siteID = 0;
            while(sites.size() < siteCount && sites.size() < jsonArray.size()){
                JSONObject element = (JSONObject) jsonArray.get(random.nextInt(jsonArray.size()));
                double lat = parseDouble(element.get("la").toString());
                if (lat < minLat) minLat = lat;
                if (lat > maxLat) maxLat = lat;
                double lng = parseDouble(element.get("lo").toString());
                if (lng < minLng) minLng = lng;
                if (lng > maxLng) maxLng = lng;
                double capacity = parseDouble(element.get("capacity").toString());
                if (capacity > maxCapacity) maxCapacity = capacity;
                //System.out.println("gonna assign id " + siteID);
                Site site = new Site(
                        lat,
                        lng,
                        capacity,
                        siteID++);
                sites.add(site);
            }

            //in case more sites needed than in databse, generating random sites
            siteCount -= sites.size();
            //System.out.println(siteCount + " is the site count");
            //System.out.println("min-max: " + minLat + " " + maxLat + " " + minLng + " " + maxLng + " " + maxCapacity);
            while(siteCount > 0) {
                double capacity = Double.MIN_VALUE + random.nextFloat() * 100;
                //System.out.println("weight to insert: " + capacity);
                Site site = new Site(
                        minLat + random.nextFloat() * (maxLat - minLat),
                        minLng + random.nextFloat() * (maxLng - minLng),
                        1,
                        siteID
                );
                sites.add(site);
                siteCount--;
            }


        } catch (ParseException | IOException e) {
            e.printStackTrace();
        }
        return sites;
    }
}
