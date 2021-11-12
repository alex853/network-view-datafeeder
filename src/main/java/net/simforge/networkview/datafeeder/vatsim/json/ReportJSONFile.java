package net.simforge.networkview.datafeeder.vatsim.json;

import com.google.gson.Gson;
import net.simforge.commons.misc.Str;
import net.simforge.networkview.core.Network;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class ReportJSONFile {
    private static final int INT_NaN = Integer.MIN_VALUE;

    private static final String CLIENTS = "Clients";

    private Network network;
    private String update;

    private List<LogEntry> log = new ArrayList<>();

    private List<ClientInfo> pilotInfos = new ArrayList<>();

    public ReportJSONFile(Network network, String data) {
        this.network = network;

        Gson gson = new Gson();

        Map map = gson.fromJson(data, Map.class);
        Map general = (Map) map.get("general");
        update = (String) general.get("update");

        List<Map> pilots = (List<Map>) map.get("pilots");
        pilots.forEach(pilot -> pilotInfos.add(parseVatsimPilotInfo(pilot)));
    }

    public Network getNetwork() {
        return network;
    }

    public String getUpdate() {
        return update;
    }

    public List<ClientInfo> getPilotInfos() {
        return Collections.unmodifiableList(pilotInfos);
    }

    public List<LogEntry> getLog() {
        return Collections.unmodifiableList(log);
    }

    private ClientInfo parseVatsimPilotInfo(Map pilotJson) {
        ClientInfo clientInfo = new ClientInfo();

        String callsign = (String) pilotJson.get("callsign");
        clientInfo.callsign                 = parseString(callsign, callsign, "Callsign", 10);

        clientInfo.cid                      = parseInt(pilotJson, "cid", callsign, "CID is incorrect", 0);
        clientInfo.clienttype               = ClientType.PILOT;

        clientInfo.latitude                 = parseCoord(pilotJson, "latitude", callsign, "Latitude", -90D, 90D);
        clientInfo.longitude                = parseCoord(pilotJson, "longitude", callsign, "Longitude", -180D, 180D);
        if (Double.isNaN(clientInfo.latitude) || Double.isNaN(clientInfo.longitude)) {
            return null;
        }

        clientInfo.altitude                 = parseAltitude(pilotJson, callsign);
        clientInfo.groundspeed              = parseGroundspeed(pilotJson, callsign);
        clientInfo.heading                  = parseHeading(pilotJson, callsign);

        clientInfo.qnhMb                    = parseInt(pilotJson, "qnh_mb", callsign, "QNH Mb is incorrect: " + pilotJson.get("qnh_mb"), 0);
        if (clientInfo.qnhMb < 0 || clientInfo.qnhMb > 2000) {
            log.add(new LogEntry(CLIENTS, callsign, "QNH MB is out of range", String.valueOf(clientInfo.qnhMb)));
            clientInfo.qnhMb = 0;
        }

        Map flightplanJson = (Map) pilotJson.get("flight_plan");
        if (flightplanJson != null) {
            clientInfo.plannedAircraft = parseString((String) flightplanJson.get("aircraft"), callsign, "Aircraft", 40);
            clientInfo.plannedDepAirport = parseString((String) flightplanJson.get("departure"), callsign, "Departure ICAO", 4);
            clientInfo.plannedDestAirport = parseString((String) flightplanJson.get("arrival"), callsign, "Destination ICAO", 4);
            clientInfo.plannedRemarks = parseString((String) flightplanJson.get("remarks"), callsign, "Remarks", 300);
        }

        return clientInfo;
    }

    private String parseString(String value, String callsign, String desc, int maxLen) {
        if (value == null) {
            return null;
        }

        value = value.trim();
        if (value.length() == 0) {
            return null;
        }

        if (value.length() > maxLen) {
            log.add(new LogEntry(CLIENTS, callsign, desc + " is too long", value));
            return value.substring(0, maxLen);
        }
        return value;
    }

    private int parseAltitude(Map pilotJson, String callsign) {
        int altitude = parseInt(pilotJson, "altitude", callsign, "Could not parse altitude value");
        if (altitude == INT_NaN) {
            return 0; // could not parse
        }
        if (altitude < -10000) {
            log.add(new LogEntry(CLIENTS, callsign, "Altitude is too small to store in DB. Restricted to -10000.", String.valueOf(altitude)));
            return 0;
        }
        if (altitude > 1000000) {
            log.add(new LogEntry(CLIENTS, callsign, "Altitude is too great to store in DB. Restricted to 1000000.", String.valueOf(altitude)));
            return 0;
        }
        return altitude;
    }

    private int parseGroundspeed(Map pilotJson, String callsign) {
        int groundspeed = parseInt(pilotJson, "groundspeed", callsign, "Could not parse groundspeed value");
        if (groundspeed == INT_NaN) {
            return 0; // could not parse
        }
        if (groundspeed < 0) {
            log.add(new LogEntry(CLIENTS, callsign, "Groundspeed is negative. Restricted to 0.", String.valueOf(groundspeed)));
            return 0;
        }
        if (groundspeed > 32767) {
            log.add(new LogEntry(CLIENTS, callsign, "Groundspeed is too great to store in DB. Restricted to 32767.", String.valueOf(groundspeed)));
            return 32767;
        }
        return groundspeed;
    }

    private int parseHeading(Map pilotJson, String callsign) {
        int heading = parseInt(pilotJson, "heading", callsign, "Could not parse heading value");
        if (heading == INT_NaN) {
            return 0; // could not parse
        }
        if (heading < 0) {
            log.add(new LogEntry(CLIENTS, callsign, "Heading is negative, reset to 0", String.valueOf(heading)));
            return 0;
        }
        if (heading > 360) {
            log.add(new LogEntry(CLIENTS, callsign, "Heading is too great, reset to 0", String.valueOf(heading)));
            return 0;
        }
        return heading;
    }

    private double parseCoord(Map json, String name, String callsign, String coordName, double min, double max) {
        Double coord = (Double) json.get(name);

        BigDecimal bigDecimalCoord = BigDecimal.valueOf(coord);
        if (bigDecimalCoord.scale() > 6) {
            log.add(new LogEntry("Clients", callsign, coordName + " has too high scale, limiting to scale 6", coord.toString()));
            coord = bigDecimalCoord.setScale(6, RoundingMode.HALF_UP).doubleValue();
        }

        if(Double.isNaN(coord))
            return coord;
        if(coord > max) {
            log.add(new LogEntry("Clients", callsign, coordName + " is greater than " + max, coord.toString()));
            return max;
        }
        if(coord < min) {
            log.add(new LogEntry("Clients", callsign, coordName + " is lower than " + min, coord.toString()));
            return min;
        }
        return coord;
    }

    private int parseInt(Map json, String name, String callsign, String msg) {
        return parseInt(json, name, callsign, msg, INT_NaN);
    }

    private int parseInt(Map json, String name, String callsign, String msg, int defaultValue) {
        Double value = (Double) json.get(name);
        if (value != null) {
            return value.intValue();
        } else {
            log.add(new LogEntry(CLIENTS, callsign, msg, null));
            return defaultValue;
        }
    }

    public class ClientInfo {
        private String callsign;
        private int cid;
        private ClientType clienttype;
        private double latitude;
        private double longitude;
        private int altitude;
        private int groundspeed;
        private String plannedAircraft;
        private String plannedDepAirport;
        private String plannedDestAirport;
        private String plannedRemarks;
        private int heading;
        private Boolean onGround;
        private Integer qnhMb;

        public int getCid() {
            return cid;
        }

        public String getCallsign() {
            return callsign;
        }

        public ClientType getClienttype() {
            return clienttype;
        }

        public double getLatitude() {
            return latitude;
        }

        public double getLongitude() {
            return longitude;
        }

        public int getAltitude() {
            return altitude;
        }

        public int getGroundspeed() {
            return groundspeed;
        }

        public int getHeading() {
            return heading;
        }

        public String getPlannedAircraft() {
            return plannedAircraft;
        }

        public String getPlannedDepAirport() {
            return plannedDepAirport;
        }

        public String getPlannedDestAirport() {
            return plannedDestAirport;
        }

        public String getPlannedRemarks() {
            return plannedRemarks;
        }

        public Boolean isOnGround() {
            return onGround;
        }

        public Integer getQnhMb() {
            return qnhMb;
        }
    }

    public enum ClientType {
        ATC,
        PILOT;

        public static ClientType parseString(String str) {
            if ("PILOT".equals(str))
                return PILOT;
            if ("ATC".equals(str))
                return ATC;
            throw new IllegalArgumentException();
        }
    }

    public static class LogEntry {
        private String section;
        private String object;
        private String msg;
        private String value;

        public LogEntry(String section, String object, String msg, String value) {
            this.section = Str.limit(section, 50);
            this.object = Str.limit(object, 50);
            this.msg = Str.limit(msg, 200);
            this.value = Str.limit(value, 1000);
        }

        public String getSection() {
            return section;
        }

        public String getObject() {
            return object;
        }

        public String getMsg() {
            return msg;
        }

        public String getValue() {
            return value;
        }
    }

}
