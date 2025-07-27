package com.luketn;

import java.io.*;
import java.lang.reflect.Array;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.nio.charset.StandardCharsets;

/**
 * Utility class for data operations.
 * Ref: https://www.ncei.noaa.gov/sites/default/files/2020-04/wodreadme_0.pdf
 */
public class DataUtil {
    private static final String[] localFiles = {
        "data-raw/ocldb1753579979.3150694.OSD.gz",
        "data-raw/ocldb1753579979.3150694.OSD2.gz",
        "data-raw/ocldb1753579979.3150694.OSD3.gz",
        "data-raw/ocldb1753579979.3150694.OSD4.gz",
        "data-raw/ocldb1753579979.3150694.CTD.gz",
        "data-raw/ocldb1753579979.3150694.CTD2.gz",
        "data-raw/ocldb1753579979.3150694.XBT.gz",
        "data-raw/ocldb1753579979.3150694.XBT2.gz",
        "data-raw/ocldb1753579979.3150694.XBT3.gz",
        "data-raw/ocldb1753579979.3150694.XBT4.gz",
        "data-raw/ocldb1753579979.3150694.MBT.gz",
        "data-raw/ocldb1753579979.3150694.MBT2.gz",
        "data-raw/ocldb1753579979.3150694.MBT3.gz",
        "data-raw/ocldb1753579979.3150694.PFL.gz",
        "data-raw/ocldb1753579979.3150694.PFL2.gz",
        "data-raw/ocldb1753579979.3150694.PFL3.gz",
        "data-raw/ocldb1753579979.3150694.PFL4.gz",
        "data-raw/ocldb1753579979.3150694.PFL5.gz",
        "data-raw/ocldb1753579979.3150694.PFL6.gz",
        "data-raw/ocldb1753579979.3150694.PFL7.gz",
        "data-raw/ocldb1753579979.3150694.PFL8.gz",
        "data-raw/ocldb1753579979.3150694.DRB.gz",
        "data-raw/ocldb1753579979.3150694.MRB.gz",
        "data-raw/ocldb1753579979.3150694.MRB2.gz",
    };


    public static void main(String[] args) throws IOException {
        for (String filePath : Arrays.stream(localFiles).filter(s -> s.contains(".OSD.gz")).toList()) {
            // Create a gunzip stream to read the gzipped file
            Path path = Paths.get(filePath);

            GZIPInputStream gzipInputStream = new GZIPInputStream(Files.newInputStream(path));
            InputStreamReader inputStreamReader = new InputStreamReader(gzipInputStream, StandardCharsets.US_ASCII);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            System.out.println("Reading file: " + filePath);
            int count=0;
            StringBuilder profileBuilder = new StringBuilder();
            while (true) {
                String line = bufferedReader.readLine();
                if (line == null || line.isEmpty()) {
                    if (profileBuilder.length() > 0) {
                        // Process the last profile if it exists
                        WodCast wodCast = parseProfile(profileBuilder.toString(), path.getFileName().toString());
                        System.out.println(wodCast);
                    }
                    break;
                } else if (line.startsWith("C")) {
                    if (profileBuilder.length() > 0) {
                        // Process the last profile if it exists
                        WodCast wodCast = parseProfile(profileBuilder.toString(), path.getFileName().toString());
                        System.out.println(wodCast);
                    }
                }
                profileBuilder.append(line);
            }
        }
    }
    public record WodCast(
            String instrumentType,
            Instant timestamp,
            String isoCountryCode,
            Float latitude,  // stored as microdegrees
            Float longitude, // stored as microdegrees
            Float degreesCelcius
    ) {}

    public static WodCast parseProfile(String line, String fileName) {
        String instrument = extractInstrument(fileName);
        System.out.println(line);
        AtomicInteger pointer = new AtomicInteger(0);
        try {
            char version = line.charAt(0);
            if (version != 'C') return null;
            pointer.getAndIncrement();

            // Profile
            String profileBytes = readField(line, pointer); //Number of bytes in profile

            // Cast number
            String castNumber = readField(line, pointer); // The WOD unique cast number, which is a variable-length field.

            // Cruise country code
            String isoCountryCode = readField(line, pointer, Optional.of(2));

            // Cruise number (unused)
            String cruiseNumber = readField(line, pointer);

            // Year, month, day, time
            Integer year = readNumericField(line, pointer, Optional.of(4));
            Integer month = readNumericField(line, pointer, Optional.of(2));
            Integer day = readNumericField(line, pointer, Optional.of(2));
            Instant timestamp = Instant.EPOCH;
            if (year != null && month != null && day != null) {
                timestamp = LocalDate.of(year, month, day)
                        .atStartOfDay()
                        .toInstant(ZoneOffset.UTC);
            }

            var time = readNumericFloat(line, pointer);


            // Latitude, longitude
            Float latitude = readNumericFloat(line, pointer);
            Float longitude = readNumericFloat(line, pointer);

            Integer numberOfLevels = readNumericField(line, pointer);
            Integer profileType = readNumericField(line, pointer, Optional.of(1));
            Integer numberOfVariables = readNumericField(line, pointer, Optional.of(2));
            if (numberOfLevels == null || numberOfVariables == null) {
                throw new IllegalArgumentException("Invalid number of levels or variables in line: " + line);
            }


            // read the actual data for each variable
            record VariableValue(
                    String variableCode,
                    Float value
            ) {}
            record VariableMeta(
                    String variableCode,
                    Integer variableQCFlag,
                    Integer numVarSpecificMeta,
                    List<VariableValue> metaValues
            ) {}
            List<VariableMeta> variableMetas = new ArrayList<>();
            for (int i = 0; i < numberOfVariables; i++) {
                boolean keepReading = true;
                while (keepReading) {
                    String variableCode = readField(line, pointer);
                    Integer variableQCFlag = readNumericField(line, pointer, Optional.of(1)); // Quality control flag for variable
                    Integer numVarSpecificMeta = readNumericField(line, pointer); // Number of variable-specific metadata (M)
                    List<VariableValue> metaValues = new ArrayList<>();
                    if (numVarSpecificMeta == 0) {
                        keepReading = true;
                    } else {
                        keepReading = false; // If M is zero, we stop reading variable-specific metadata
                        //todo: Fix this
                        // read numVarSpecificMeta (M) values
                        for (int j = 0; j < numVarSpecificMeta; j++) {
                            String varSpecificCode = readField(line, pointer);
                            Float value = readNumericFloat(line, pointer);
                            metaValues.add(new VariableValue(varSpecificCode, value));
                        }
                    }
                    variableMetas.add(new VariableMeta(variableCode, variableQCFlag, numVarSpecificMeta, metaValues));
                }
            }


            Float temperature = null;
            for (VariableMeta variableMeta : variableMetas) {
                if (variableMeta.variableCode.equals("1")) { // Variable code "1" is temperature
                    for (VariableValue variableValue : variableMeta.metaValues) {
                        temperature = variableValue.value;
                        break; // Found the temperature, no need to continue
                    }
                    if (temperature != null) break; // Exit outer loop if temperature found
                }
            }

            return new WodCast(
                    instrument,
                    timestamp,
                    isoCountryCode,
                    latitude,
                    longitude,
                    temperature
            );
        } catch (Exception ex) {
            System.out.println("Error parsing line: " + line + " in file: " + fileName +
                    " at position: " + pointer.get() + " with error: " + ex.getMessage());
            return null;
        }
    }

    private static String extractInstrument(String fn) {
        // fileName like ...ocldb...OSD.gz → instrument "OSD"
        int dot = fn.lastIndexOf('.');
        if (dot > 0) {
            String core = fn.substring(0, dot);
            int dot2 = core.lastIndexOf('.');
            return core.substring(dot2 + 1);
        }
        return fn;
    }

    private static String readField(String line, AtomicInteger pointer) {
        return readField(line, pointer, Optional.empty());
    }
    private static String readField(String line, AtomicInteger pointer, Optional<Integer> fixedFieldLength) {
        int fieldLength;
        if (fixedFieldLength.isPresent()) {
            fieldLength = fixedFieldLength.get();
        } else {
            String fieldLengthString = line.substring(pointer.get(), pointer.get()+1);
            fieldLength = Integer.parseInt(fieldLengthString);
            if (fieldLength <= 0) return null;
            pointer.getAndIncrement();
        }
        String value = line.substring(pointer.get(), pointer.get()+fieldLength);
        pointer.addAndGet(fieldLength);
        return value;
    }

    private static Integer readNumericField(String line, AtomicInteger pointer) {
        return readNumericField(line, pointer, Optional.empty());
    }
    private static Integer readNumericField(String line, AtomicInteger pointer, Optional<Integer> fixedFieldLength) {
        String stringValue = readField(line, pointer, fixedFieldLength);
        if (stringValue == null) return null;
        return Integer.parseInt(stringValue.trim());
    }

    private static Float readNumericFloat(String line, AtomicInteger pointer) throws IOException {
        if (line.charAt(pointer.get()) == '-') {
            pointer.getAndIncrement(); // skip the '-'
            return null;
        }
        int significantDigits = Integer.parseInt(line.substring(pointer.get(), pointer.incrementAndGet()));
        int totalDigits = Integer.parseInt(line.substring(pointer.get(), pointer.incrementAndGet()));
        int precision = Integer.parseInt(line.substring(pointer.get(), pointer.incrementAndGet()));
        String valueString = line.substring(pointer.get(), pointer.get() + totalDigits);
        //e.g. valueString = "-5513" for a latitude of -55.13 degrees
        Float value = valueString.isEmpty() ? null : Float.parseFloat(valueString) / (float) Math.pow(10, precision);
        pointer.addAndGet(totalDigits);
        return value;
    }


    // Class representing a format item with a field name and byte length (0 for variable length)
    public class FormatItem {
        private final String fieldName;
        private final int length;
        public FormatItem(String fieldName, int length) {
            this.fieldName = fieldName;
            this.length = length;
        }
        public String getFieldName() { return fieldName; }
        public int getLength() { return length; }
    }

    // List defining the ASCII format for the WOD Primary Header (prhFormat)
    List<FormatItem> prhFormat = List.of(
            new FormatItem("Version", 1),       // Field 1: WOD Version identifier (1 byte, e.g. "C" for WOD13/WOD18):contentReference[oaicite:2]{index=2}
            new FormatItem("BytesInNext", 1),   // Field 2: Bytes in next field (1 byte, length of ProfileBytes field):contentReference[oaicite:3]{index=3}
            new FormatItem("ProfileBytes", 0),  // Field 3: Bytes in profile (length determined by field 2):contentReference[oaicite:4]{index=4}
            new FormatItem("BytesInNext", 1),   // Field 4: Bytes in next field (1 byte, length of CastNumber field):contentReference[oaicite:5]{index=5}
            new FormatItem("CastNumber", 0),    // Field 5: WOD unique cast number (variable length, from field 4):contentReference[oaicite:6]{index=6}
            new FormatItem("Country", 2),       // Field 6: Country Code (2 bytes, ISO country code):contentReference[oaicite:7]{index=7}
            new FormatItem("BytesInNext", 1),   // Field 7: Bytes in next field (1 byte, length of CruiseNumber field):contentReference[oaicite:8]{index=8}
            new FormatItem("CruiseNumber", 0),  // Field 8: Cruise Number (variable length, from field 7):contentReference[oaicite:9]{index=9}
            new FormatItem("Year", 4),          // Field 9: Year (4 bytes, e.g. "2025"):contentReference[oaicite:10]{index=10}
            new FormatItem("Month", 2),         // Field 10: Month (2 bytes, zero-padded):contentReference[oaicite:11]{index=11}
            new FormatItem("Day", 2),           // Field 11: Day (2 bytes, zero-padded; may be 00 if unknown):contentReference[oaicite:12]{index=12}
            // Field 12: Time (if missing, SignifDigs = '-' and skip Value):contentReference[oaicite:13]{index=13}
            new FormatItem("SignifDigs", 1),    // 12a. Significant digits of Time (1 byte):contentReference[oaicite:14]{index=14}
            new FormatItem("TotalDigits", 1),   // 12b. Total digits of Time (1 byte):contentReference[oaicite:15]{index=15}
            new FormatItem("Precision", 1),     // 12c. Precision of Time (1 byte, number of decimal places):contentReference[oaicite:16]{index=16}
            new FormatItem("Time", 0),          // 12d. Time value (variable length, digits count from 12b):contentReference[oaicite:17]{index=17}
            // Field 13: Latitude (if missing, SignifDigs = '-' and skip value):contentReference[oaicite:18]{index=18}
            new FormatItem("SignifDigs", 1),    // 13a. Significant digits of Latitude (1 byte):contentReference[oaicite:19]{index=19}
            new FormatItem("TotalDigits", 1),   // 13b. Total digits of Latitude (1 byte)
            new FormatItem("Precision", 1),     // 13c. Precision of Latitude (1 byte)
            new FormatItem("Latitude", 0),      // 13d. Latitude value (variable length, from 13b)
            // Field 14: Longitude (if missing, SignifDigs = '-' and skip value):contentReference[oaicite:20]{index=20}
            new FormatItem("SignifDigs", 1),    // 14a. Significant digits of Longitude (1 byte)
            new FormatItem("TotalDigits", 1),   // 14b. Total digits of Longitude (1 byte)
            new FormatItem("Precision", 1),     // 14c. Precision of Longitude (1 byte)
            new FormatItem("Longitude", 0),     // 14d. Longitude value (variable length, from 14b)

            new FormatItem("BytesInNext", 1),   // Field 15: Bytes in next field (1 byte, length of NumberOfLevels):contentReference[oaicite:21]{index=21}
            new FormatItem("NumberOfLevels", 0),// Field 16: Number of Levels (L) in profile (variable length, from field 15):contentReference[oaicite:22]{index=22}
            new FormatItem("ProfileType", 1),   // Field 17: Profile type (1 byte, 0 = observed, 1 = standard level):contentReference[oaicite:23]{index=23}
            new FormatItem("NumVariables", 2),  // Field 18: # of Variables in profile (N) (2 bytes):contentReference[oaicite:24]{index=24}

            // Fields 19–23 are repeated N times (for each variable in the profile):contentReference[oaicite:25]{index=25}
            new FormatItem("BytesInNext", 1),   // Field 19: Bytes in next field (1 byte, length of VariableCode):contentReference[oaicite:26]{index=26}
            new FormatItem("VariableCode", 0),  // Field 20: Variable code (variable length, from field 19):contentReference[oaicite:27]{index=27}
            new FormatItem("VariableQCFlag", 1),// Field 21: Quality control flag for variable (1 byte):contentReference[oaicite:28]{index=28}
            new FormatItem("BytesInNext", 1),   // Field 22: Bytes in next field (1 byte, length of NumVarSpecificMeta):contentReference[oaicite:29]{index=29}
            new FormatItem("NumVarSpecificMeta", 0), // Field 23: Number of variable-specific metadata (M) (variable length, from field 22):contentReference[oaicite:30]{index=30}
            // Fields 24–25 are repeated M times for each variable (variable-specific metadata entries):contentReference[oaicite:31]{index=31}
            new FormatItem("BytesInNext", 1),   // Field 24: Bytes in next field (1 byte, length of VarSpecificCode):contentReference[oaicite:32]{index=32}
            new FormatItem("VarSpecificCode", 0),// Field 25: Variable-specific code (variable length, from field 24):contentReference[oaicite:33]{index=33}
            new FormatItem("SignifDigs", 1),    // 25a. Significant digits of metadata value (1 byte):contentReference[oaicite:34]{index=34}
            new FormatItem("TotalDigits", 1),   // 25b. Total digits of metadata value (1 byte)
            new FormatItem("Precision", 1),     // 25c. Precision of metadata value (1 byte)
            new FormatItem("Value", 0)          // 25d. Metadata value (variable length, from 25b)
    );


    /**
     * URLs sent by noaa.gov based on a query for temperature data from 1771 to today.
     */
    private static String[] noaQueryExtractUrls = {
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.OSD.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.OSD2.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.OSD3.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.OSD4.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.CTD.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.CTD2.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.XBT.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.XBT2.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.XBT3.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.XBT4.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.MBT.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.MBT2.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.MBT3.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.PFL.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.PFL2.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.PFL3.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.PFL4.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.PFL5.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.PFL6.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.PFL7.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.PFL8.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.DRB.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.MRB.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.MRB2.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.APB.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.APB2.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.APB3.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.APB4.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.UOR.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.SUR.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.GLD.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.GLD2.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.GLD3.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.GLD4.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.GLD5.gz",
            "https://www.ncei.noaa.gov/access/world-ocean-database-select/OCLdb_output/ocldb1753579979.3150694.GLD6.gz"
    };
    private static void downloadTemperatures() throws IOException {
        Files.createDirectories(Paths.get("data-raw"));
        for (String file : noaQueryExtractUrls) {
            //download files into the 'data-raw' directory
            String fileName = file.substring(file.lastIndexOf('/') + 1);
            String filePath = "data-raw/" + fileName;
            try {
                URL url = new URL(file);
                try (InputStream in = url.openStream()) {
                    Files.copy(in, Paths.get(filePath), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    System.out.println("Downloaded: " + filePath);
                }
            } catch (Exception e) {
                System.err.println("Failed to download " + file + ": " + e.getMessage());
            }
        }
    }
}
