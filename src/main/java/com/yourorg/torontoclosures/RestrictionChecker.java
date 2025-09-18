package com.yourorg.torontoclosures;

import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import com.microsoft.aad.msal4j.*;

import okhttp3.*;

import org.locationtech.jts.geom.*;
import org.locationtech.proj4j.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * RestrictionChecker — improved
 */
public class RestrictionChecker {

    private static final OkHttpClient http = new OkHttpClient.Builder().build();
    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final String GRAPH_SCOPE = "https://graph.microsoft.com/.default";
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    @FunctionName("PollRodars")
    public void run(
            @TimerTrigger(name = "timerInfo", schedule = "0 */15 * * * *") String timerInfo,
            final ExecutionContext context) {

        final Logger log = context.getLogger();
        try {
            log.info("PollRodars triggered at " + Instant.now().toString());

            final String clientId = getEnv("CLIENT_ID");
            final String clientSecret = getEnv("CLIENT_SECRET");
            final String tenantId = getEnv("TENANT_ID");
            final String spSiteUrl = getEnv("SP_SITE_URL");
            final String siteListSites = getEnv("SP_LIST_SITES_NAME", "SiteMaster");
            final String siteListClosures = getEnv("SP_LIST_CLOSURES_NAME", "ClosureStatus");
            final String cityQueryUrl = getEnv("CITY_LAYER_QUERY", "https://gis.toronto.ca/arcgis/rest/services/cot_geospatial2/FeatureServer/76/query?where=1%3D1&outFields=*&f=geojson");
            final int bufferMeters = Integer.parseInt(getEnv("BUFFER_METERS", "30"));

            String accessToken = acquireToken(clientId, clientSecret, tenantId, log);

            SiteInfo siteInfo = getSiteInfoFromUrl(spSiteUrl, accessToken, log);
            if (siteInfo == null) {
                log.severe("Failed to resolve SharePoint site id from SP_SITE_URL: " + spSiteUrl);
                return;
            }
            log.info("Resolved site: " + siteInfo.siteId + " (hostname=" + siteInfo.hostname + " path=" + siteInfo.sitePath + ")");

            String listSitesId = getListId(siteInfo.siteId, siteListSites, accessToken, log);
            String listClosuresId = getListId(siteInfo.siteId, siteListClosures, accessToken, log);
            if (listSitesId == null || listClosuresId == null) {
                log.severe("Could not find list IDs. SiteMaster id=" + listSitesId + ", ClosureStatus id=" + listClosuresId);
                return;
            }
            log.info("Found lists: SiteMaster=" + listSitesId + " ClosureStatus=" + listClosuresId);

            List<SiteRow> siteRows = fetchSiteMasterItems(siteInfo.siteId, listSitesId, accessToken, log);
            log.info("Loaded SiteMaster rows: " + siteRows.size());

            List<ClosureFeature> closures = fetchArcGisFeatures(cityQueryUrl, bufferMeters, log);
            log.info("Parsed closures: " + closures.size());

            // Project transforms: WGS84 <-> WebMercator for meter buffers
            CRSFactory crsFactory = new CRSFactory();
            CoordinateTransformFactory ctFactory = new CoordinateTransformFactory();
            CoordinateReferenceSystem wgs84 = crsFactory.createFromName("EPSG:4326");
            CoordinateReferenceSystem webmerc = crsFactory.createFromName("EPSG:3857");
            CoordinateTransform toMerc = ctFactory.createTransform(wgs84, webmerc);

            GeometryFactory geometryFactory = new GeometryFactory();

            // Build buffered polygons in mercator space
            List<Geometry> bufferedPolygons = new ArrayList<>();
            List<ClosureFeature> activeClosures = new ArrayList<>();
            Instant now = Instant.now();

            for (ClosureFeature f : closures) {
                if (!isActiveAtInstant(f, now)) {
                    continue; // not active at this time
                }

                // convert endpoints to mercator coordinates (x,y in meters)
                ProjCoordinate p1 = new ProjCoordinate(f.lon1, f.lat1);
                ProjCoordinate p2 = new ProjCoordinate(f.lon2, f.lat2);
                ProjCoordinate m1 = new ProjCoordinate();
                ProjCoordinate m2 = new ProjCoordinate();
                toMerc.transform(p1, m1);
                toMerc.transform(p2, m2);

                Coordinate[] coords = new Coordinate[]{
                        new Coordinate(m1.x, m1.y),
                        new Coordinate(m2.x, m2.y)
                };
                LineString line = geometryFactory.createLineString(coords);

                // buffer distance in meters = bufferMeters (radius)
                double bufferDist = ((double) bufferMeters);
                Geometry poly = line.buffer(bufferDist, 8); // 8 quadrant segments
                bufferedPolygons.add(poly);
                activeClosures.add(f);
            }

            // Fetch ClosureStatus items once and map site_id -> itemId to avoid N fetches
            Map<Integer, String> closureStatusMap = fetchClosureStatusMap(siteInfo.siteId, listClosuresId, accessToken, log);

            // For each site row, check collisions
            for (SiteRow site : siteRows) {
                ProjCoordinate sp = new ProjCoordinate(site.lng, site.lat);
                ProjCoordinate sm = new ProjCoordinate();
                toMerc.transform(sp, sm);
                Point pt = geometryFactory.createPoint(new Coordinate(sm.x, sm.y));

                List<Integer> conflictsIndex = new ArrayList<>();
                for (int i = 0; i < bufferedPolygons.size(); i++) {
                    Geometry poly = bufferedPolygons.get(i);
                    // use covers so points on boundary count as conflicts
                    if (poly.covers(pt)) {
                        conflictsIndex.add(i);
                    }
                }

                boolean isBlocked = !conflictsIndex.isEmpty();
                int numConflicts = conflictsIndex.size();
                String conflictsJson = buildConflictsJson(activeClosures, conflictsIndex);

                String existingItemId = closureStatusMap.get(site.siteId);

                upsertClosureStatus(siteInfo.siteId, listClosuresId, site, isBlocked, numConflicts, conflictsJson, accessToken, log, existingItemId);
                log.info("Site id=" + site.siteId + " blocked=" + isBlocked + " conflicts=" + numConflicts);
            }

            log.info("PollRodars finished at " + Instant.now().toString());

        } catch (Throwable t) {
            log.severe("PollRodars failed: " + t.toString());
            StringWriter sw = new StringWriter();
            t.printStackTrace(new PrintWriter(sw));
            log.severe(sw.toString());
            throw new RuntimeException(t);
        }
    }

    // ---- Helper data classes ----
    private static class SiteInfo {
        String hostname;
        String sitePath; // e.g. /sites/Andrews.engineer
        String siteId;
    }

    private static class SiteRow {
        int siteId;
        String siteName;
        double lat;
        double lng;
        String itemId; // optional closure list item id if desired
    }

    private static class ClosureFeature {
        double lat1, lon1, lat2, lon2;
        Instant start, end;
        JsonNode originalProps;
    }

    // ---- Helpers ----

    private static String getEnv(String key) {
        String v = System.getenv(key);
        if (v == null) throw new IllegalStateException("Missing environment variable: " + key);
        return v;
    }

    private static String getEnv(String key, String fallback) {
        String v = System.getenv(key);
        return (v == null) ? fallback : v;
    }

    private static String acquireToken(String clientId, String clientSecret, String tenantId, final Logger log) throws Exception {
        ConfidentialClientApplication app = ConfidentialClientApplication.builder(
                        clientId,
                        ClientCredentialFactory.createFromSecret(clientSecret))
                .authority("https://login.microsoftonline.com/" + tenantId)
                .build();

        ClientCredentialParameters params = ClientCredentialParameters.builder(Collections.singleton(GRAPH_SCOPE)).build();
        IAuthenticationResult result = app.acquireToken(params).get();
        if (result == null) throw new RuntimeException("Failed to acquire token");
        log.info("Acquired Graph token, expiresAt: " + result.expiresOnDate());
        return result.accessToken();
    }

    private static SiteInfo getSiteInfoFromUrl(String spSiteUrl, String accessToken, final Logger log) throws IOException {
        String url = spSiteUrl.trim();
        if (!url.startsWith("http")) url = "https://" + url;
        String hostname;
        String path;
        try {
            java.net.URL u = new java.net.URL(url);
            hostname = u.getHost();
            path = u.getPath();
        } catch (Exception ex) {
            log.severe("Invalid SP_SITE_URL: " + spSiteUrl);
            return null;
        }
        String graphUrl = "https://graph.microsoft.com/v1.0/sites/" + hostname + ":" + path + ":";

        Request req = new Request.Builder().url(graphUrl).addHeader("Authorization", "Bearer " + accessToken).get().build();
        try (Response resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful()) {
                log.severe("Graph get site failed: " + resp.code() + " " + resp.message() + " body=" + (resp.body()!=null?resp.body().string():"<empty>"));
                return null;
            }
            JsonNode root = mapper.readTree(resp.body().byteStream());
            SiteInfo si = new SiteInfo();
            si.hostname = hostname;
            si.sitePath = path;
            si.siteId = root.path("id").asText(null);
            return si;
        }
    }

    private static String getListId(String siteId, String listName, String accessToken, final Logger log) throws IOException {
        String url = "https://graph.microsoft.com/v1.0/sites/" + siteId + "/lists?$filter=displayName eq '" + listName.replace("'", "''") + "'";
        Request req = new Request.Builder().url(url).addHeader("Authorization", "Bearer " + accessToken).get().build();
        try (Response resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful()) {
                log.severe("Graph get lists failed: " + resp.code() + " " + resp.message());
                return null;
            }
            JsonNode root = mapper.readTree(resp.body().byteStream());
            JsonNode vals = root.path("value");
            if (vals.isArray() && vals.size() > 0) {
                return vals.get(0).path("id").asText(null);
            } else {
                log.warning("List not found: " + listName);
                return null;
            }
        }
    }

    private static List<SiteRow> fetchSiteMasterItems(String siteId, String listId, String accessToken, final Logger log) throws IOException {
        List<SiteRow> out = new ArrayList<>();
        String url = "https://graph.microsoft.com/v1.0/sites/" + siteId + "/lists/" + listId + "/items?expand=fields&$top=999";
        Request req = new Request.Builder().url(url).addHeader("Authorization", "Bearer " + accessToken).get().build();
        try (Response resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful()) {
                log.severe("Graph get SiteMaster items failed: " + resp.code() + " " + resp.message());
                return out;
            }
            JsonNode root = mapper.readTree(resp.body().byteStream());
            JsonNode vals = root.path("value");
            if (vals.isArray()) {
                for (JsonNode item : vals) {
                    JsonNode fields = item.path("fields");
                    SiteRow r = new SiteRow();
                    r.itemId = item.path("id").asText(null);
                    r.siteName = text(fields, "name", "Title", "site_name");
                    String sid = text(fields, "site_id", "SiteId", "ID");
                    if (sid != null && !sid.isEmpty()) {
                        try { r.siteId = Integer.parseInt(sid); } catch (Exception ignored) { r.siteId = -1; }
                    } else {
                        r.siteId = -1;
                    }
                    String latS = text(fields, "lat", "latitude", "Lat", "Latitude");
                    String lngS = text(fields, "lng", "longitude", "Lng", "Lon", "Longitude");
                    if (latS != null && lngS != null) {
                        try { r.lat = Double.parseDouble(latS); r.lng = Double.parseDouble(lngS); }
                        catch (Exception ex) { log.warning("Bad lat/lng for site " + r.siteName + " lat=" + latS + " lng=" + lngS); continue; }
                    } else {
                        log.warning("Site row missing lat/lng: " + r.siteName);
                        continue;
                    }
                    out.add(r);
                }
            }
        }
        return out;
    }

    private static String text(JsonNode node, String... keys) {
        if (node == null) return null;
        for (String k : keys) {
            JsonNode v = node.path(k);
            if (!v.isMissingNode() && !v.isNull()) {
                return v.asText();
            }
        }
        return null;
    }

    private static List<ClosureFeature> fetchArcGisFeatures(String cityQueryUrl, int bufferMeters, final Logger log) throws IOException {
        List<ClosureFeature> list = new ArrayList<>();
        String url = cityQueryUrl;
        Request req = new Request.Builder().url(url).get().build();
        try (Response resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful()) {
                log.severe("ArcGIS query failed: " + resp.code() + " " + resp.message());
                return list;
            }
            JsonNode root = mapper.readTree(resp.body().byteStream());
            JsonNode features = root.path("features");
            if (!features.isArray()) return list;
            for (JsonNode feat : features) {
                JsonNode props = feat.path("properties");
                ClosureFeature f = new ClosureFeature();
                f.lat1 = tryDouble(props, "FROM_ROAD_LATITUDE", "from_road_latitude", "from_lat");
                f.lon1 = tryDouble(props, "FROM_ROAD_LONGITUDE", "from_road_longitude", "from_lon");
                f.lat2 = tryDouble(props, "TO_ROAD_LATITUDE", "to_road_latitude", "to_lat");
                f.lon2 = tryDouble(props, "TO_ROAD_LONGITUDE", "to_road_longitude", "to_lon");
                f.originalProps = props;
                f.start = tryParseInstant(props, "START_DATE", "start_date", "start");
                f.end = tryParseInstant(props, "END_DATE", "end_date", "end");
                if (Double.isFinite(f.lat1) && Double.isFinite(f.lon1) && Double.isFinite(f.lat2) && Double.isFinite(f.lon2)) {
                    list.add(f);
                }
            }
        }
        return list;
    }

    private static double tryDouble(JsonNode props, String... keys) {
        for (String k : keys) {
            JsonNode v = props.path(k);
            if (!v.isMissingNode() && !v.isNull()) {
                if (v.isNumber()) return v.asDouble();
                String s = v.asText();
                if (s == null) continue;
                try { return Double.parseDouble(s); } catch (Exception ignore) {}
            }
        }
        return Double.NaN;
    }

    private static Instant tryParseInstant(JsonNode props, String... keys) {
        for (String k : keys) {
            JsonNode v = props.path(k);
            if (!v.isMissingNode() && !v.isNull()) {
                if (v.isNumber()) {
                    try { return Instant.ofEpochMilli(v.asLong()); } catch (Exception ignored) {}
                } else {
                    String s = v.asText();
                    if (s == null) continue;
                    try { long maybe = Long.parseLong(s); return Instant.ofEpochMilli(maybe); } catch (Exception ignored) {}
                    try { return Instant.parse(s); } catch (Exception ignored) {}
                    try { return ZonedDateTime.parse(s).toInstant(); } catch (Exception ignored) {}
                }
            }
        }
        return null;
    }

    private static boolean isActiveAtInstant(ClosureFeature f, Instant now) {
        if (f.start == null && f.end == null) return true; // always active
        if (f.start != null && f.end != null) {
            return !(now.isBefore(f.start) || now.isAfter(f.end)); // inclusive
        }
        if (f.start != null) {
            return !now.isBefore(f.start);
        }
        // f.end != null
        return !now.isAfter(f.end);
    }

    private static String buildConflictsJson(List<ClosureFeature> activeClosures, List<Integer> idxs) {
        try {
            List<Map<String,Object>> rows = new ArrayList<>();
            for (int i : idxs) {
                ClosureFeature f = activeClosures.get(i);
                Map<String,Object> m = new HashMap<>();
                m.put("from", Arrays.asList(f.lat1, f.lon1));
                m.put("to", Arrays.asList(f.lat2, f.lon2));
                m.put("start", f.start==null?null:f.start.toString());
                m.put("end", f.end==null?null:f.end.toString());
                rows.add(m);
            }
            return mapper.writeValueAsString(rows);
        } catch (Exception ex) {
            return "[]";
        }
    }

    private static Map<Integer, String> fetchClosureStatusMap(String siteId, String closureListId, String accessToken, final Logger log) throws IOException {
        Map<Integer, String> map = new HashMap<>();
        String urlAll = "https://graph.microsoft.com/v1.0/sites/" + siteId + "/lists/" + closureListId + "/items?expand=fields&$top=999";
        Request reqAll = new Request.Builder().url(urlAll).addHeader("Authorization", "Bearer " + accessToken).get().build();
        try (Response resp = http.newCall(reqAll).execute()) {
            if (!resp.isSuccessful()) {
                log.severe("Graph get ClosureStatus items failed: " + resp.code() + " " + resp.message());
                return map;
            }
            JsonNode root = mapper.readTree(resp.body().byteStream());
            JsonNode vals = root.path("value");
            if (vals.isArray()) {
                for (JsonNode it : vals) {
                    JsonNode fields = it.path("fields");
                    String sid = text(fields, "site_id", "SiteId", "ID");
                    if (sid != null) {
                        try {
                            int key = Integer.parseInt(sid);
                            map.put(key, it.path("id").asText(null));
                        } catch (Exception ignored) {}
                    }
                }
            }
        }
        return map;
    }

    private static void upsertClosureStatus(String siteId, String closureListId, SiteRow site, boolean isBlocked, int numConflicts, String conflictsJson, String accessToken, final Logger log, String existingItemId) throws IOException {
        Map<String,Object> fields = new HashMap<>();
        fields.put("site_id", site.siteId);
        fields.put("site_name", site.siteName);
        fields.put("is_blocked", isBlocked);
        fields.put("num_conflicts", numConflicts);
        fields.put("conflicts_json", conflictsJson);
        fields.put("last_checked_utc", Instant.now().toString());

        if (existingItemId != null) {
            String patchUrl = "https://graph.microsoft.com/v1.0/sites/" + siteId + "/lists/" + closureListId + "/items/" + existingItemId + "/fields";
            RequestBody body = RequestBody.create(mapper.writeValueAsString(fields), JSON);
            Request patch = new Request.Builder().url(patchUrl).addHeader("Authorization", "Bearer " + accessToken).patch(body).build();
            try (Response resp = http.newCall(patch).execute()) {
                if (!resp.isSuccessful()) {
                    log.severe("PATCH ClosureStatus failed: " + resp.code() + " " + resp.message() + " body=" + (resp.body()!=null?resp.body().string():""));
                } else {
                    log.info("Updated ClosureStatus item " + existingItemId + " for site " + site.siteId);
                }
            }
        } else {
            String postUrl = "https://graph.microsoft.com/v1.0/sites/" + siteId + "/lists/" + closureListId + "/items";
            Map<String,Object> payload = new HashMap<>();
            payload.put("fields", fields);
            RequestBody body = RequestBody.create(mapper.writeValueAsString(payload), JSON);
            Request post = new Request.Builder().url(postUrl).addHeader("Authorization", "Bearer " + accessToken).post(body).build();
            try (Response resp = http.newCall(post).execute()) {
                if (!resp.isSuccessful()) {
                    log.severe("POST ClosureStatus failed: " + resp.code() + " " + resp.message() + " body=" + (resp.body()!=null?resp.body().string():""));
                } else {
                    log.info("Created ClosureStatus for site " + site.siteId);
                }
            }
        }
    }

    public static void main(String[] args) {
        // create a logger for local runs
        java.util.logging.Logger log = java.util.logging.Logger.getLogger("RestrictionCheckerMain");

        // create a simple ExecutionContext implementation for local invocation
        ExecutionContext ctx = new SimpleExecutionContext(log);

        // call the existing run method (timerInfo string + context)
        try {
            new RestrictionChecker().run("manual-invocation", ctx);
        } catch (Exception ex) {
            log.severe("Local run failed: " + ex.toString());
            ex.printStackTrace();
            System.exit(1);
        }
    }

    // Minimal ExecutionContext implementation used only for local CLI runs
    private static class SimpleExecutionContext implements ExecutionContext {
        private final java.util.logging.Logger logger;
        SimpleExecutionContext(java.util.logging.Logger logger) { this.logger = logger; }

        @Override
        public java.util.logging.Logger getLogger() { return logger; }

        @Override
        public String getInvocationId() { return "local-invocation"; }

        @Override
        public String getFunctionName() { return "PollRodars"; }
    }
}
