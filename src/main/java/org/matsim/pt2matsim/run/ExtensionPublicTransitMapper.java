package org.matsim.pt2matsim.run;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.network.Network;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.pt.transitSchedule.api.TransitLine;
import org.matsim.pt.transitSchedule.api.TransitRoute;
import org.matsim.pt.transitSchedule.api.TransitSchedule;
import org.matsim.pt.transitSchedule.api.TransitScheduleFactory;
import org.matsim.pt2matsim.config.PublicTransitMappingConfigGroup;
import org.matsim.pt2matsim.mapping.PTMapper;
import org.matsim.pt2matsim.tools.NetworkTools;
import org.matsim.pt2matsim.tools.ScheduleTools;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ExtensionPublicTransitMapper (parallel & batched)
 * - Maps TransitRoutes per shape_id
 * - Runs mapping in parallel using 16 threads
 * - Writes one temporary schedule file for every 10 shapes
 * - Logs live batch and global progress
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
        // Load config
        PublicTransitMappingConfigGroup config = PublicTransitMappingConfigGroup.loadConfig(configFile);

        // Load schedule & network
        TransitSchedule schedule = ScheduleTools.readTransitSchedule(config.getInputScheduleFile());
        Network network = NetworkTools.readNetwork(config.getInputNetworkFile());

        // Create temp directory
        String tempDir = (config.getOutputScheduleFile() != null)
                ? new File(config.getOutputScheduleFile()).getParent() + File.separator + "tempSchedules"
                : "tempSchedules";
        File tempDirFile = new File(tempDir);
        if (!tempDirFile.exists()) tempDirFile.mkdirs();
        log.info("Temporary schedules will be saved in: " + tempDir);

        // Group routes by shape_id
        Map<String, List<Map.Entry<TransitLine, TransitRoute>>> shapeToRoutes = new HashMap<>();
        for (TransitLine line : schedule.getTransitLines().values()) {
            for (TransitRoute route : line.getRoutes().values()) {
                String shapeId = route.getAttributes().getAttribute("shape_id") != null
                        ? route.getAttributes().getAttribute("shape_id").toString()
                        : line.getId() + "_" + route.getId();

                shapeToRoutes.computeIfAbsent(shapeId, k -> new ArrayList<>())
                        .add(new AbstractMap.SimpleEntry<>(line, route));
            }
        }

        int totalShapes = shapeToRoutes.size();
        log.info("Total unique shape_ids to map: " + totalShapes);

        // Prepare thread pool and progress counter
        int numThreads = Math.min(16, Runtime.getRuntime().availableProcessors());
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger globalCounter = new AtomicInteger(0);

        List<String> shapeIds = new ArrayList<>(shapeToRoutes.keySet());
        int batchSize = 10;
        int totalBatches = (int) Math.ceil((double) totalShapes / batchSize);

        for (int batchStart = 0; batchStart < shapeIds.size(); batchStart += batchSize) {
            int batchEnd = Math.min(batchStart + batchSize, shapeIds.size());
            List<String> batchShapeIds = new ArrayList<>(shapeIds.subList(batchStart, batchEnd));
            int batchIndex = batchStart / batchSize + 1;

            Future<?> future = executor.submit(() -> {
                try {
                    TransitScheduleFactory scheduleFactory = ScenarioUtils.createScenario(ConfigUtils.createConfig())
                            .getTransitSchedule().getFactory();
                    TransitSchedule tempSchedule = scheduleFactory.createTransitSchedule();

                    int batchProgress = 0;
                    for (String shapeId : batchShapeIds) {
                        batchProgress++;
                        int overall = globalCounter.incrementAndGet();
                        double percent = (overall * 100.0 / totalShapes);

                        log.info(String.format(
                                "[Batch %d/%d] Processing shape_id=%s (%d/%d in batch, global: %d/%d, %.2f%%)",
                                batchIndex, totalBatches, shapeId,
                                batchProgress, batchShapeIds.size(),
                                overall, totalShapes, percent));

                        List<Map.Entry<TransitLine, TransitRoute>> routes = shapeToRoutes.get(shapeId);
                        Map.Entry<TransitLine, TransitRoute> firstPair = routes.get(0);
                        TransitLine line = firstPair.getKey();
                        TransitRoute route = firstPair.getValue();

                        // Reuse existing line if already present
                        TransitLine tempLine = tempSchedule.getTransitLines().get(line.getId());
                        if (tempLine == null) {
                            tempLine = scheduleFactory.createTransitLine(line.getId());
                            tempSchedule.addTransitLine(tempLine);
                        }
                        tempLine.addRoute(route);

                        // Copy stop facilities safely
                        route.getStops().forEach(stop -> {
                            var facilityId = stop.getStopFacility().getId();
                            if (!tempSchedule.getFacilities().containsKey(facilityId)) {
                                tempSchedule.addStopFacility(schedule.getFacilities().get(facilityId));
                            }
                        });
                    }

                    // Map this batch’s schedule
                    PTMapper.mapScheduleToNetwork(tempSchedule, network, config);

                    // Write out batch
                    String safeFileName = "batch_" + batchIndex + "_shapes_" + batchShapeIds.size() + ".xml";
                    String outputFile = tempDir + File.separator + safeFileName;
                    ScheduleTools.writeTransitSchedule(tempSchedule, outputFile);

                    log.info(String.format(
                            "✅ [Batch %d/%d] Completed (%d shapes). Cumulative progress: %d/%d (%.2f%%) -> %s",
                            batchIndex, totalBatches, batchShapeIds.size(),
                            globalCounter.get(), totalShapes,
                            (globalCounter.get() * 100.0 / totalShapes),
                            outputFile));

                } catch (Exception e) {
                    log.error("❌ Error processing batch " + batchIndex, e);
                }
            });

            futures.add(future);
        }

        // Wait for all tasks
        for (Future<?> future : futures) {
            future.get();
        }

        executor.shutdown();
        log.info("✅ Completed mapping all batches (" + totalShapes + " shapes total).");

        // Step 3: Write output schedule & network
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
/** Test
 *
 */
