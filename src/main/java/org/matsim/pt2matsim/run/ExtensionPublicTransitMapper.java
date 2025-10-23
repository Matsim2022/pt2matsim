package org.matsim.pt2matsim.run;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.network.Network;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.pt.transitSchedule.api.*;
import org.matsim.pt2matsim.config.PublicTransitMappingConfigGroup;
import org.matsim.pt2matsim.mapping.PTMapper;
import org.matsim.pt2matsim.tools.NetworkTools;
import org.matsim.pt2matsim.tools.ScheduleTools;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

/**
 * ExtensionPublicTransitMapper (parallel + batched)
 * - Maps TransitRoutes per shape_id
 * - Executes mapping in parallel using up to 16 threads
 * - Writes one temporary schedule file for every 10 shapes
 * - Ensures no repeated shape_id across batch files
 */
public final class ExtensionPublicTransitMapper {

    protected static Logger log = LogManager.getLogger(ExtensionPublicTransitMapper.class);

    private ExtensionPublicTransitMapper() {}

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        if (args.length == 1) {
            run(args[0]);
        } else {
            throw new IllegalArgumentException("Public Transit Mapping config file as argument needed");
        }
    }

    public static void run(String configFile) throws InterruptedException, ExecutionException {
        // --- Load config ---
        PublicTransitMappingConfigGroup config = PublicTransitMappingConfigGroup.loadConfig(configFile);

        // --- Load input schedule and network ---
        TransitSchedule schedule = ScheduleTools.readTransitSchedule(config.getInputScheduleFile());
        Network network = NetworkTools.readNetwork(config.getInputNetworkFile());

        // --- Prepare temp directory ---
        String tempDir = (config.getOutputScheduleFile() != null)
                ? new File(config.getOutputScheduleFile()).getParent() + File.separator + "tempSchedules"
                : "tempSchedules";
        File tempDirFile = new File(tempDir);
        if (!tempDirFile.exists()) tempDirFile.mkdirs();
        log.info("Temporary schedules will be saved in: " + tempDir);

        // --- Create factory for temp schedules ---
        Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());
        TransitScheduleFactory scheduleFactory = scenario.getTransitSchedule().getFactory();

        // --- Group TransitRoutes by shape_id ---
        Map<String, List<Map.Entry<TransitLine, TransitRoute>>> shapeToRoutes = new LinkedHashMap<>();
        for (TransitLine line : schedule.getTransitLines().values()) {
            for (TransitRoute route : line.getRoutes().values()) {
                String shapeId = route.getAttributes().getAttribute("shape_id") != null
                        ? route.getAttributes().getAttribute("shape_id").toString()
                        : route.getId().toString();
                shapeToRoutes.computeIfAbsent(shapeId, k -> new ArrayList<>())
                        .add(new AbstractMap.SimpleEntry<>(line, route));
            }
        }

        log.info("Total unique shape_ids to map: " + shapeToRoutes.size());

        // --- Thread pool setup ---
        int numThreads = Math.min(16, Runtime.getRuntime().availableProcessors());
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> futures = new ArrayList<>();

        // --- Batching ---
        List<String> shapeIds = new ArrayList<>(shapeToRoutes.keySet());
        int batchSize = 10;
        int totalBatches = (int) Math.ceil((double) shapeIds.size() / batchSize);

        for (int batchStart = 0; batchStart < shapeIds.size(); batchStart += batchSize) {
            int batchEnd = Math.min(batchStart + batchSize, shapeIds.size());
            List<String> batchShapeIds = new ArrayList<>(shapeIds.subList(batchStart, batchEnd));
            int batchIndex = batchStart / batchSize + 1;

            Future<?> future = executor.submit(() -> {
                try {
                    TransitSchedule tempSchedule = scheduleFactory.createTransitSchedule();

                    for (String shapeId : batchShapeIds) {
                        List<Map.Entry<TransitLine, TransitRoute>> routes = shapeToRoutes.get(shapeId);
                        if (routes == null || routes.isEmpty()) continue;

                        Map.Entry<TransitLine, TransitRoute> firstPair = routes.get(0);
                        TransitLine line = firstPair.getKey();
                        TransitRoute originalRoute = firstPair.getValue();

                        // ✅ Reuse line if already exists in tempSchedule
                        TransitLine tempLine = tempSchedule.getTransitLines().get(line.getId());
                        if (tempLine == null) {
                            tempLine = scheduleFactory.createTransitLine(line.getId());
                            tempSchedule.addTransitLine(tempLine);
                        }

                        // ✅ Clone route (avoid sharing across threads)
                        TransitRoute clonedRoute = scheduleFactory.createTransitRoute(
                                originalRoute.getId(),
                                originalRoute.getRoute(),
                                new ArrayList<>(originalRoute.getStops()),
                                originalRoute.getTransportMode()
                        );

                        // Copy departures
                        originalRoute.getDepartures().forEach((depId, dep) -> {
                            Departure clonedDep = scheduleFactory.createDeparture(dep.getId(), dep.getDepartureTime());
                            clonedRoute.addDeparture(clonedDep);
                        });

                        // Copy attributes
                        originalRoute.getAttributes().getAsMap().forEach(clonedRoute.getAttributes()::putAttribute);

                        tempLine.addRoute(clonedRoute);

                        // Copy facilities safely
                        clonedRoute.getStops().forEach(stop -> {
                            var facilityId = stop.getStopFacility().getId();
                            if (!tempSchedule.getFacilities().containsKey(facilityId)) {
                                tempSchedule.addStopFacility(schedule.getFacilities().get(facilityId));
                            }
                        });

                        log.info("Mapped shape_id: " + shapeId + " in batch " + batchIndex);
                    }

                    // --- Run mapping ---
                    PTMapper.mapScheduleToNetwork(tempSchedule, network, config);

                    // --- Write batch output ---
                    String safeFileName = "batch_" + batchIndex + "_shapes_" + batchShapeIds.size() + ".xml";
                    String outputFile = tempDir + File.separator + safeFileName;
                    ScheduleTools.writeTransitSchedule(tempSchedule, outputFile);
                    log.info("✅ Wrote batch " + batchIndex + "/" + totalBatches + " (" + batchShapeIds.size() + " shapes) -> " + outputFile);

                } catch (Exception e) {
                    log.error("❌ Error processing batch " + batchIndex, e);
                }
            });

            futures.add(future);
        }

        // --- Wait for all threads to finish ---
        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();
        log.info("✅ Completed mapping all " + totalBatches + " batches (" + shapeIds.size() + " shapes total).");

        // --- Write final output schedule and network ---
        if (config.getOutputNetworkFile() != null && config.getOutputScheduleFile() != null) {
            log.info("Writing final schedule and network...");
            try {
                ScheduleTools.writeTransitSchedule(schedule, config.getOutputScheduleFile());
                NetworkTools.writeNetwork(network, config.getOutputNetworkFile());
            } catch (Exception e) {
                log.error("Cannot write to output directory!", e);
            }

            if (config.getOutputStreetNetworkFile() != null) {
                NetworkTools.writeNetwork(
                        NetworkTools.createFilteredNetworkByLinkMode(network, Collections.singleton("car")),
                        config.getOutputStreetNetworkFile());
            }
        } else {
            log.info("No output paths defined, schedule and network are not written to files.");
        }
    }
}

/** Test 2
 *
 */
