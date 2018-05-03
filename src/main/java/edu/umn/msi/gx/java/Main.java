package edu.umn.msi.gx.java;

import edu.umn.msi.gx.database.DatabaseManager;
import edu.umn.msi.gx.database.MetaTableManager;
import edu.umn.msi.gx.fasta.FastaParse;
import edu.umn.msi.gx.mgf.MGFReader;
import edu.umn.msi.gx.verification.IDMapping;
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) throws Exception {

        Logger logger = LogManager.getLogger("Main Driver");

        Options options = new Options();
        options.addOption("h", "help", false, "show help.");
        options.addOption("mzid", true, "Full path to the mzIdentML file.");
        options.addOption("mzidDisplayName", true, "Galaxy display name for msIdentML file.");

        Option o = new Option("scanfiles", true, "List of mgf input files.");
        o.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(o);
        Option oName = new Option("scanFilesDisplayName", true, "Galaxy display names for scan input files");
        oName.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(oName);
        Option rxList = new Option("protein_id_regex", true, "Custom REGEX for extracting ID from FASTA files.");
        rxList.setArgs(Option.UNLIMITED_VALUES);
        options.addOption(rxList);

        options.addOption("fasta", true, "Fasta file for protein sequences.");
        options.addOption("dbname", true, "Full path name for the sqlite3 database.");
        options.addOption("numthreads", true, "Number of processing threads");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("h")) {
            logger.info("Please use options");
            System.exit(0);
        }

        int num_threads = 5; //Default
        if (cmd.hasOption("numthreads")) {
            num_threads = Integer.valueOf(cmd.getOptionValue("numthreads"));
            if (num_threads < 2) {
                num_threads = 2;
            }
        }

        String mzIdentFilePath;
        if (cmd.hasOption("mzid")) {
            mzIdentFilePath = cmd.getOptionValue("mzid");
        } else {
            throw new MissingArgumentException("mzIdentML file must be present");
        }

        String databasePathName;
        if (cmd.hasOption("dbname")) {
            databasePathName = cmd.getOptionValue("dbname");
        } else {
            throw new MissingArgumentException("Path to database file must be present");
        }

        List<String> scanFiles = new ArrayList<>(Arrays.asList(cmd.getOptionValues("scanfiles")));

        String fastaFilePathName = null;
        if (cmd.hasOption("fasta")) {
            fastaFilePathName = cmd.getOptionValue("fasta");
        }

        IDMapping idMapping = new IDMapping();
        if (cmd.hasOption("mzidDisplayName")) {
            idMapping.mzIDnetNameMap.put(cmd.getOptionValue("mzidDisplayName"), cmd.getOptionValue("mzid"));
        } else {
            idMapping.mzIDnetNameMap.put(cmd.getOptionValue("mzid"), cmd.getOptionValue("mzid"));
        }

        if (cmd.hasOption("scanFilesDisplayName")) {
            List<String> dScanNames = Arrays.asList(cmd.getOptionValues("scanFilesDisplayName"));
            List<String> scanNames = Arrays.asList(cmd.getOptionValues("scanfiles"));
            for (int idx = 0; idx < dScanNames.size(); idx++) {
                idMapping.scanFilesNameMap.put(scanNames.get(idx), dScanNames.get(idx));
            }
        } else {
            List<String> scanNames = Arrays.asList(cmd.getOptionValues("scanfiles"));
            for (String s : scanNames) {
                idMapping.scanFilesNameMap.put(s, s);
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(num_threads - 1);
        List<ThreadedXMLReader> xmlTasks = new ArrayList<>();
        List<MGFReader> mgfReaders = new ArrayList<>();

        logger.info("Starting mzident parsing.");

        //Build a series of xml parsers, each with a specific type of content handler.
        ThreadedXMLReader t1 = new ThreadedXMLReader(mzIdentFilePath, new SequenceCollectionHandler());
        ThreadedXMLReader t2 = new ThreadedXMLReader(mzIdentFilePath, new ProteinAmbiguityHandler());
        ThreadedXMLReader t3 = new ThreadedXMLReader(mzIdentFilePath, new SpectrumIdentificationHandler());
        ThreadedXMLReader t4 = new ThreadedXMLReader(mzIdentFilePath, new AnalysisProtocolHandler());
        //All parsers go into collection
        xmlTasks.add(t1);
        xmlTasks.add(t2);
        xmlTasks.add(t3);
        xmlTasks.add(t4);
        for (ThreadedXMLReader t : xmlTasks) {
            executor.execute(t);
        }
        //wait  . . .
        executor.shutdown();
        executor.awaitTermination(xmlTasks.size() * 120, TimeUnit.MINUTES); //2 minutes per xml handler
        logger.info("Finished mzIdent parsing");

        DatabaseManager dbMgr = new DatabaseManager(databasePathName);
        dbMgr.createNewDatabase();

        for (ThreadedXMLReader t : xmlTasks) {
            t.setDbMgr(dbMgr);
            t.generateTablesAndData();
        }

        MetaTableManager.setDatabaseManager(dbMgr);
        MetaTableManager.createEncodedSequences();
        MetaTableManager.createCountsTable();

        logger.info("Finished mzIdent database DDL.");
        logger.info("Starting mgf parsing");

        /**
         * The mgf files will have many more scans than were used in an identification.
         * We do not want the overhead of storing peak/intensity values for scans that are
         * never used.
         *
         * So, id the peaks we need.
         */
        List<String> usedScans = new ArrayList<>();
        SpectrumIdentificationHandler sih = (SpectrumIdentificationHandler) t3.contentHandler;

        for (String key : sih.getSpectrumIdentificationResults().keySet()) {
            SpectrumIdentificationResult r = sih.getSpectrumIdentificationResults().get(key);
            usedScans.add(r.spectrumTitle);
        }
        //Thread reading of MGF files.
        for (String mgfName : scanFiles) {
            MGFReader m = new MGFReader(mgfName);
            m.setIdMapping(idMapping);
            m.setUsedScans(usedScans);
            mgfReaders.add(m);
        }

        executor = Executors.newFixedThreadPool(num_threads - 1);
        logger.info("Begin MSScan parsing");

        for (MGFReader mr : mgfReaders) {
            executor.execute(mr);
        }
        executor.shutdown();
        executor.awaitTermination((mgfReaders.size() * 30), TimeUnit.MINUTES); //5 min max per mgf file.

        for (MGFReader mr : mgfReaders) {
            MetaTableManager.setMGFData(mr.getScans());
            mr.clearScans();
        }

        logger.info("Completed MSScan parsing");

        MetaTableManager.addMGFScores();
        MetaTableManager.addEnhancedFragmentScores(sih.getSpectrumIdentificationResults());
        MetaTableManager.generateScoreSummaryTable(sih.getSpectrumIdentificationResults().keySet().size());
        MetaTableManager.createPSMTable();

        if (fastaFilePathName != null) {
            logger.info("Reading fasta file {} for protein sequences", fastaFilePathName);

            SequenceCollectionHandler sci = (SequenceCollectionHandler) t1.contentHandler;
            FastaParse fp = new FastaParse(sci.getDBSequenceIDs());

            if (cmd.hasOption("protein_id_regex")) {
                List<String> lstRX = new ArrayList<>();
                lstRX.addAll(Arrays.asList(cmd.getOptionValues("protein_id_regex")));
                fp.setIDRegEx(lstRX);
            }
            Map<String, String> seqs = fp.parseFASTA(fastaFilePathName);
            logger.info("Finished fasta parsing.");
            MetaTableManager.addProteinSequences(seqs);
        }
        MetaTableManager.createPepToProteinTable();

        dbMgr.conn.close();

        logger.info("Closing db conn and exiting");
    }
}
