package deduplication.preparation.addressnormalization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.GetRequest;
import deduplication.similarities.utils.StringSimilarityMeasure;
import deduplication.similarities.utils.string.hybrid.MongeElkanSymmetric;
import org.apache.commons.csv.CSVFormat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;


/**
 * Created by jokoum on 11/30/16.
 */
public class NominatimSanitizer {
    protected static CSVFormat CSV_FORMAT = CSVFormat.TDF.withFirstRecordAsHeader().withIgnoreEmptyLines().withDelimiter('\t');
    protected static Gson gson = new GsonBuilder().enableComplexMapKeySerialization().setPrettyPrinting().create();

    /* http://stackoverflow.com/questions/3000214/java-http-client-request-with-defined-timeout */
    // set the connection timeout value to 30 seconds (30000 milliseconds)

    StringSimilarityMeasure sim;

    public NominatimSanitizer(StringSimilarityMeasure sim) {
        Unirest.setTimeouts(5000, 20000);
        Unirest.setConcurrency(1000, 200);
        this.sim = sim;
    }

    public void close() {
        try {
            Unirest.shutdown();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    protected String getNominatimHTTPRequestQuery (String address, String countryCode) {
    protected String getGeocodeQuery (String address, String countryCode) {
        /* NOTE: Replace the gaps with '+' */
//        String serverUrlStr = "http://172.16.64.11/nominatim";
        String serverUrlStr = "http://nominatim.openstreetmap.org";
//        String searchUrlStr = serverUrlStr + "/search?format=json";
        String searchUrlStr = serverUrlStr + "/?format=json";
        searchUrlStr += "&q=" + address.replaceAll(" ", "+");
        if (countryCode != null) {
            searchUrlStr += "&country=" + countryCode;
        }
        searchUrlStr += "&accept-language=en";
        searchUrlStr += "&addressdetails=1";
        searchUrlStr += "&limit=1";
//        searchUrlStr += "&extratags=1";
//        searchUrlStr += "&namedetails=1";

        System.out.println(searchUrlStr);

        return searchUrlStr;
    }

    public NominatimResponse normalizeAddress(String address, String countryCode) throws IOException, URISyntaxException, ExecutionException, InterruptedException {
        URL searchUrl = new URL(getGeocodeQuery(address, countryCode));
        GetRequest request = Unirest.get(searchUrl.toURI().toString());
        request.header("Accept-Language", "en;en-us;en-gb");

        long start = System.currentTimeMillis();
        String result = null;

        try {
            result = request.asString().getBody();
        } catch (UnirestException e) {
            e.printStackTrace();
        }

        long finish = System.currentTimeMillis();

        System.out.println(result);
        NominatimResponse nr = NominatimResponse.valueOf(result).get(0);
        return nr;
    }


    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        NominatimSanitizer ns = new NominatimSanitizer(
                new MongeElkanSymmetric.MongeElkanSymmetricLevenshtein("mes", Boolean.TRUE)
        );


        long start = System.currentTimeMillis();
        String address = "walter-klausch-str 1";
        String countryCode = null;
        try {
            NominatimResponse response = ns.normalizeAddress(address, countryCode);
            System.out.println(response.getAddress().getRoad());
        } catch (URISyntaxException | ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        long finish = System.currentTimeMillis();
        System.out.println("time: " + (finish-start));
    }

}
