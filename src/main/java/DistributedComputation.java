import com.opencsv.CSVWriter;
import mpi.MPI;
import java.awt.*;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

public class DistributedComputation {
    static ArrayList<Site> sitePoints;
    static ArrayList<Cluster> clusters;
    private static int time;
    private static long memory;
    private static int loopCounter;
    private static double[] clusterBuffer, siteBuffer;
    private static int[] clusterChunkSizes, clusterDispls, siteChunkSizes, siteDispls;
    private static boolean[] updateFlags;
    private static Timestamp start, end;
    //params: 5 10000 100 100000 100


    public static void main(String[] args) throws IOException {
        //System.out.println(Arrays.toString(args));

        CSVWriter file = new CSVWriter(new FileWriter("test_results.csv"));
        file.writeNext(new String[]{"id", "type", "runtime", "memory", "points", "centeroids"}, false);

        int numberOfIterations = Integer.parseInt(args[6]);
        int siteStep = Integer.parseInt(args[7]);
        int clusterStep = Integer.parseInt(args[8]);
        int siteFixed = Integer.parseInt(args[9]);
        int clusterFixed = Integer.parseInt(args[10]);

        int id = 0;

        //varying sites
        for(int i = 1; i <= numberOfIterations; i++) {
            int siteCount = i * siteStep;
            compute(args, siteCount, clusterFixed);
            if (Objects.equals(Thread.currentThread().getName(), "0")) {
                System.out.println("Clusters calculated in: " + time + "ms. Performed " + loopCounter + " loops.");
                file.writeNext(new String[]{
                        Integer.toString(id),
                        "distributed",
                        Double.toString(time),
                        Double.toString(memory),
                        Integer.toString(siteCount),
                        Integer.toString(clusterFixed)
                }, false);
                id++;
            }
        }

        //varying clusters
        for(int i = 1; i <= numberOfIterations; i++) {
            int clusterCount = i * clusterStep;
            compute(args, siteFixed, clusterCount);
            if (Objects.equals(Thread.currentThread().getName(), "0")) {
                System.out.println("Clusters calculated in: " + time + "ms. Performed " + loopCounter + " loops.");
                file.writeNext(new String[]{
                        Integer.toString(id),
                        "distributed",
                        Double.toString(time),
                        Double.toString(memory),
                        Integer.toString(siteFixed),
                        Integer.toString(clusterCount)
                }, false);
                id++;
            }
        }

        file.close();
    }

    static void compute(String[] args, int siteCount, int clusterCount) {
        MPI.Init(args);

        int rank = MPI.COMM_WORLD.Rank();
        int commSize = MPI.COMM_WORLD.Size();
        //System.out.println("i am " + rank + " out of " + commSize);
        long startMemory = 0, endMemory = 0;

        clusters = new ArrayList<>();
        sitePoints = new ArrayList<>();
        //[id1, lat1, long1, id2, lat2, long2, id3, lat3, long3,...]
        clusterBuffer = new double[3 * clusterCount];
        //[id1, lat1, long1, w1, clusterID1, id2, lat2, long2, w2, clusterID2, ...]
        siteBuffer = new double[5 * siteCount];
        updateFlags = new boolean[commSize];


        //calculating the number of sites one process should deal with
        //rounding up so no element left in case of division with weird numbers
        int siteChunkSize = siteCount / commSize;
        int clusterChunkSize = clusterCount / commSize;

        //flag
        boolean[] continuing = new boolean[1];
        continuing[0] = false;


        //preparations for varying length scatter and gather
        clusterChunkSizes = new int[commSize];
        clusterDispls = new int[commSize];

        int chunkCounter = 0;
        int remaining = clusterChunkSize == 0 ? clusterCount : clusterCount % clusterChunkSize;
        //making varying chunk size
        for (int i = 0; i < commSize; i++) {
            clusterChunkSizes[i] = clusterChunkSize * 3;
            if (remaining > 0) {
                clusterChunkSizes[i] += 3;
                remaining--;
            }
            clusterDispls[i] = chunkCounter * 3;
            chunkCounter += chunkCounter;
        }


        siteChunkSizes = new int[commSize];
        siteDispls = new int[commSize];
        chunkCounter = 0;
        remaining = siteChunkSize == 0 ? siteCount : siteCount % siteChunkSize;
        //making varying chunk size
        for (int i = 0; i < commSize; i++) {
            siteChunkSizes[i] = siteChunkSize * 5;
            if (remaining > 0) {
                siteChunkSizes[i] += 5;
                remaining--;
            }
            siteDispls[i] = chunkCounter;
            chunkCounter += siteChunkSizes[i];
        }

        double[] siteElements = new double[siteChunkSizes[rank]];
        double[] clusterElements = new double[clusterChunkSizes[rank]];
        boolean[] flag = new boolean[1];


        if (rank == 0) {
            //setting sets up
            sitePoints = SiteLoader.getInstance().loadSites(siteCount);

            //getting clusters up
            Random rand = new Random();
            HashSet<Integer> randInts = new HashSet<>();
            //get random non-repeating indices
            while (randInts.size() < clusterCount) {
                randInts.add(rand.nextInt(sitePoints.size()));
            }
            //create a set of colors of corresponding size for coloring the clusters
            HashSet<Color> colors = new HashSet<>();
            for (int i = 0; i < clusterCount; i++) {
                colors.add(new Color(rand.nextFloat(), rand.nextFloat(), rand.nextFloat()));
            }
            //add sites at given indexes as initial clusters
            Iterator<Color> colorIterator = colors.iterator();
            int clusterID = 0;
            for (Integer i : randInts) {
                clusters.add(new Cluster(sitePoints.get(i), colorIterator.next(), clusterID++));
            }

            //SERIALIZE
            //turning cluster and site instances into "triples" and "quintuples"
            //[id1, lat1, long1, id2, lat2, long2, id3, lat3, long3,...]
            for (Cluster cluster : clusters
            ) {
                int id = cluster.getClusterID();
                clusterBuffer[id * 3] = cluster.getClusterID();
                clusterBuffer[id * 3 + 1] = cluster.getCentroid().getLatitude();
                clusterBuffer[id * 3 + 2] = cluster.getCentroid().getLongitude();
            }
            //[lat1, long1, clusterID1, weight1, id1, lat2, long2, clusterID2, weight2, id2 ...]
            for (int i = 0; i < siteCount; i++) {
                Site site = sitePoints.get(i);
                siteBuffer[i * 5] = site.getSiteID();
                siteBuffer[i * 5 + 1] = site.getLatitude();
                siteBuffer[i * 5 + 2] = site.getLongitude();
                siteBuffer[i * 5 + 3] = site.getWeight();
                siteBuffer[i * 5 + 4] = site.getClusterID();
            }
            loopCounter = 0;
        }
        start = new Timestamp(System.currentTimeMillis());

        MPI.COMM_WORLD.Barrier();
        do {
            if (rank == 0) loopCounter++;
            updateFlags = new boolean[commSize];
            continuing[0] = false;
            flag[0] = true;


            //send all clusters to all
            MPI.COMM_WORLD.Bcast(clusterBuffer, 0, clusterBuffer.length, MPI.DOUBLE, 0);
            //send chunks of sites to respective proc
            MPI.COMM_WORLD.Scatterv(siteBuffer, 0, siteChunkSizes, siteDispls, MPI.DOUBLE, siteElements, 0, siteChunkSizes[rank], MPI.DOUBLE, 0);
            //send flags to respective process
            MPI.COMM_WORLD.Scatter(updateFlags, 0, 1, MPI.BOOLEAN, flag, 0, 1, MPI.BOOLEAN, 0);

            //TODO check if start of memory measuring is ok
            //--------------- memory start
            if(rank == 0) startMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            //-------------------------------


            //BINDING STEP
            for (int i = 0; i < siteElements.length; i += 5) {
                int minClusterID = -1;
                double min = Double.MAX_VALUE;
                for (int j = 0; j < clusterBuffer.length; j += 3) {
                    double distance = Math.sqrt(Math.pow((clusterBuffer[j + 1] - siteElements[i + 1]), 2)) +
                            Math.sqrt(Math.pow((clusterBuffer[j + 2] - siteElements[i + 2]), 2));
                    if (distance < min) {
                        min = distance;
                        minClusterID = j / 3;
                    }
                }
                if(!flag[0]) {
                    if(siteElements[i + 4] != minClusterID) flag[0] = true;
                }
                siteElements[i + 4] = minClusterID;
            }

            MPI.COMM_WORLD.Allgatherv(siteElements, 0, siteElements.length, MPI.DOUBLE, siteBuffer, 0, siteChunkSizes, siteDispls, MPI.DOUBLE);

            //STOPPING CONDITION

            MPI.COMM_WORLD.Gather(flag, 0, 1, MPI.BOOLEAN, updateFlags, 0, 1, MPI.BOOLEAN, 0);
            if (rank == 0) {
                for (boolean updateFlag : updateFlags) {
                    if (updateFlag) {
                        continuing[0] = true;
                        break;
                    }
                }
            }

            //UPDATE STEP
            MPI.COMM_WORLD.Scatterv(clusterBuffer, 0, clusterChunkSizes, clusterDispls, MPI.DOUBLE, clusterElements, 0, clusterChunkSizes[rank], MPI.DOUBLE, 0);

            for (int i = 0; i < clusterElements.length; i += 3) { //iterating through cluster
                double sumX = 0, sumY = 0, weight = 0;
                for (int j = 0; j < siteBuffer.length; j += 5) { //incrementing by 5, since each group of five elements is data for one site
                    //check if current site belongs to given cluster
                    if ((int) siteBuffer[j + 4] == clusterElements[i]) {
                        sumX += siteBuffer[j + 1] * siteBuffer[j + 3];
                        sumY += siteBuffer[j + 2] * siteBuffer[j + 3];
                        weight += siteBuffer[j + 3];
                    }
                }
                clusterElements[i + 1] = sumX / weight;
                clusterElements[i + 2] = sumY / weight;
            }

            MPI.COMM_WORLD.Allgatherv(clusterElements, 0, clusterElements.length, MPI.DOUBLE, clusterBuffer, 0, clusterChunkSizes, clusterDispls, MPI.DOUBLE);

            //checking stopping conditions
            if (rank == 0) continuing[0] = continuing[0] && (loopCounter < 1000);
            MPI.COMM_WORLD.Bcast(continuing, 0, 1, MPI.BOOLEAN, 0);

        } while (continuing[0]);

        if (rank == 0) {

            //TODO check if end of memory measuring is OK
            //------------- memory end
            endMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            //-------------------------

            memory = endMemory - startMemory;
            end = new Timestamp(System.currentTimeMillis());
            time = (int) (end.getTime() - start.getTime());
        }
        MPI.Finalize();
    }
}
