/* *********************************************************************** *
 * project: org.matsim.*
 * *********************************************************************** *
 *                                                                         *
 * copyright       : (C) 2016 by the members listed in the COPYING,        *
 *                   LICENSE and WARRANTY file.                            *
 * email           : info at matsim dot org                                *
 *                                                                         *
 * *********************************************************************** *
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *   See also COPYING, LICENSE and WARRANTY file                           *
 *                                                                         *
 * *********************************************************************** */

package org.matsim.pt2matsim.run;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.LogManager;
import org.matsim.api.core.v01.Id;
import org.matsim.core.utils.geometry.geotools.MGC;
import org.matsim.core.utils.io.IOUtils;
import org.matsim.pt.transitSchedule.api.TransitLine;
import org.matsim.pt.transitSchedule.api.TransitRoute;
import org.matsim.pt.transitSchedule.api.TransitSchedule;
import org.matsim.pt2matsim.gtfs.AdditionalTransitLineInfo;
import org.matsim.pt2matsim.gtfs.GtfsConverter;
import org.matsim.pt2matsim.gtfs.GtfsFeed;
import org.matsim.pt2matsim.gtfs.GtfsFeedImpl;
import org.matsim.pt2matsim.tools.ScheduleTools;

import com.opencsv.CSVWriter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.matsim.pt2matsim.gtfs.GtfsConverter.*;

/**
 * Extended GTFS converter that also generates a CSV summary of all TransitLines and Routes.
 *
 * Based on {@link Gtfs2TransitSchedule}, extended to output:
 * - transitLineId, transitRouteId, description, transportMode
 * - summary table (total lines, routes, unique descriptions, unique transport modes)
 *
 */
public final class Gtfs2TransitScheduleExtension {

    protected static Logger log = LogManager.getLogger(Gtfs2TransitScheduleExtension.class);

    private static final String INFO_OUTPUT_OPTION_SCHEDULE = "schedule";

    private Gtfs2TransitScheduleExtension() {
    }

    public static void main(final String[] args) {
        if (args.length == 7) {
            run(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
        } else if (args.length == 6) {
            run(args[0], args[1], args[2], args[3], args[4], args[5], null);
        } else if (args.length == 5) {
            run(args[0], args[1], args[2], args[3], args[4], null, null);
        } else if (args.length == 4) {
            run(args[0], args[1], args[2], args[3], null, null, null);
        } else {
            throw new IllegalArgumentException("Wrong number of input arguments.");
        }
    }

    /**
     * Extended run method that adds generation of CSV summary.
     */
    public static void run(String gtfsFolder, String sampleDayParam, String outputCoordinateSystem,
                           String scheduleFile, String vehicleFile, String additionalLineInfoFile, String csvSummaryFile) {
        Configurator.setLevel(LogManager.getLogger(MGC.class).getName(), Level.ERROR);

        // check sample day parameter
        if (!isValidSampleDayParam(sampleDayParam)) {
            throw new IllegalArgumentException("Sample day parameter not recognized! Allowed: date in format \"yyyymmdd\", " +
                    DAY_WITH_MOST_SERVICES + ", " + DAY_WITH_MOST_TRIPS + ", " + ALL_SERVICE_IDS);
        }
        String param = sampleDayParam == null ? DAY_WITH_MOST_TRIPS : sampleDayParam;

        // load gtfs files
        GtfsFeed gtfsFeed = new GtfsFeedImpl(gtfsFolder);

        // convert to transit schedule
        GtfsConverter converter = new GtfsConverter(gtfsFeed);
        converter.convert(param, outputCoordinateSystem);

        if (additionalLineInfoFile != null && additionalLineInfoFile.equals(INFO_OUTPUT_OPTION_SCHEDULE)) {
            writeInfoToSchedule(converter.getSchedule(), converter.getAdditionalLineInfo());
        }

        // write schedule and vehicle files
        ScheduleTools.writeTransitSchedule(converter.getSchedule(), scheduleFile);
        if (vehicleFile != null) {
            ScheduleTools.writeVehicles(converter.getVehicles(), vehicleFile);
        }

        // write line info file (if applicable)
        if (additionalLineInfoFile != null && !additionalLineInfoFile.equals(INFO_OUTPUT_OPTION_SCHEDULE)) {
            writeInfoToFile(additionalLineInfoFile, converter.getAdditionalLineInfo());
        }

        // ✅ Write CSV summary of schedule
        if (csvSummaryFile != null) {
            writeScheduleSummaryCsv(csvSummaryFile, converter.getSchedule());
        }
    }

    private static boolean isValidSampleDayParam(String check) {
        if (!check.equals(ALL_SERVICE_IDS) && !check.equals(DAY_WITH_MOST_TRIPS) && !check.equals(DAY_WITH_MOST_SERVICES)) {
            try {
                LocalDate.of(Integer.parseInt(check.substring(0, 4)),
                        Integer.parseInt(check.substring(4, 6)),
                        Integer.parseInt(check.substring(6, 8)));
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    private static void writeInfoToSchedule(TransitSchedule schedule, Map<Id<TransitLine>, AdditionalTransitLineInfo> infos) {
        for (TransitLine line : schedule.getTransitLines().values()) {
            AdditionalTransitLineInfo info = infos.get(line.getId());
            if (info == null) {
                log.warn("Could not find info for transit line " + line.getId().toString());
                return;
            }
            line.getAttributes().putAttribute(AdditionalTransitLineInfo.INFO_COLUMN_LONGNAME, info.getLongName());
            line.getAttributes().putAttribute(AdditionalTransitLineInfo.INFO_COLUMN_TYPE, info.getRouteType().name);
            line.getAttributes().putAttribute(AdditionalTransitLineInfo.INFO_COLUMN_DESCRIPTION, info.getRouteDescription());
            line.getAttributes().putAttribute(AdditionalTransitLineInfo.INFO_COLUMN_AGENCY_ID, info.getAgencyId());
            line.getAttributes().putAttribute(AdditionalTransitLineInfo.INFO_COLUMN_AGENCY_NAME, info.getAgencyName());
            line.getAttributes().putAttribute(AdditionalTransitLineInfo.INFO_COLUMN_AGENCY_URL, info.getAgencyURL());
        }
    }

    private static void writeInfoToFile(String filename, Map<Id<TransitLine>, AdditionalTransitLineInfo> infos) {
        try (CSVWriter writer = new CSVWriter(IOUtils.getBufferedWriter(filename))) {
            writer.writeNext(new String[]{
                    AdditionalTransitLineInfo.INFO_COLUMN_ID,
                    AdditionalTransitLineInfo.INFO_COLUMN_SHORTNAME,
                    AdditionalTransitLineInfo.INFO_COLUMN_LONGNAME,
                    AdditionalTransitLineInfo.INFO_COLUMN_TYPE,
                    AdditionalTransitLineInfo.INFO_COLUMN_DESCRIPTION,
                    AdditionalTransitLineInfo.INFO_COLUMN_AGENCY_ID,
                    AdditionalTransitLineInfo.INFO_COLUMN_AGENCY_NAME,
                    AdditionalTransitLineInfo.INFO_COLUMN_AGENCY_URL,
                    AdditionalTransitLineInfo.INFO_COLUMN_NUM_TRANSIT_ROUTES,
                    AdditionalTransitLineInfo.INFO_COLUMN_NUM_TOTAL_DEPARTURES
            });
            for (AdditionalTransitLineInfo info : infos.values()) {
                writer.writeNext(new String[]{
                        info.getId(),
                        info.getShortName(),
                        info.getLongName(),
                        info.getRouteType().name,
                        info.getRouteDescription(),
                        info.getAgencyId(),
                        info.getAgencyName(),
                        info.getAgencyURL(),
                        Integer.toString(info.getNumberOfTransitRoutes()),
                        Integer.toString(info.getTotalNumberOfDepartures())
                });
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes CSV summary of all transit lines and routes,
     * plus summary statistics (total lines, routes, unique descriptions, unique transport modes).
     */
    private static void writeScheduleSummaryCsv(String filename, TransitSchedule schedule) {
        try {
            Files.createDirectories(Paths.get(filename).getParent());
        } catch (IOException e) {
            log.warn("Could not create directory for " + filename);
        }

        try (CSVWriter writer = new CSVWriter(IOUtils.getBufferedWriter(filename))) {
            writer.writeNext(new String[]{"transitLineId", "transitRouteId", "description", "transportMode"});

            int totalLines = 0;
            int totalRoutes = 0;
            Set<String> uniqueDescriptions = new HashSet<>();
            Set<String> uniqueTransportModes = new HashSet<>();

            for (TransitLine line : schedule.getTransitLines().values()) {
                totalLines++;
                for (TransitRoute route : line.getRoutes().values()) {
                    totalRoutes++;
                    String desc = route.getDescription() == null ? "" : route.getDescription();
                    String mode = route.getTransportMode() == null ? "" : route.getTransportMode();

                    if (!desc.isEmpty()) uniqueDescriptions.add(desc);
                    if (!mode.isEmpty()) uniqueTransportModes.add(mode);

                    writer.writeNext(new String[]{
                            line.getId().toString(),
                            route.getId().toString(),
                            desc,
                            mode
                    });
                }
            }

            writer.writeNext(new String[]{});
            writer.writeNext(new String[]{"--- Summary ---"});
            writer.writeNext(new String[]{"Total Transit Lines", String.valueOf(totalLines)});
            writer.writeNext(new String[]{"Total Transit Routes", String.valueOf(totalRoutes)});
            writer.writeNext(new String[]{"Total Unique Descriptions", String.valueOf(uniqueDescriptions.size())});
            writer.writeNext(new String[]{"Total Unique Transport Modes", String.valueOf(uniqueTransportModes.size())});

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        log.info("✅ Wrote schedule summary CSV: " + filename);
    }
}
