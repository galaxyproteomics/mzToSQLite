package edu.umn.msi.gx.mgf;

import edu.umn.msi.gx.java.CVPair;
import edu.umn.msi.gx.java.FloatPair;
import edu.umn.msi.gx.verification.IDMapping;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MGFReader implements Runnable {

    /**
     * Example snip
     *
     COM=Conversion to mascot generic
     CHARGE=2+ and 3+
     BEGIN IONS
     PEPMASS=312.519409
     CHARGE=3+
     SCANS=899
     TITLE=Mo_Tai_iTRAQ_f1.00899.00899.3
     110.059761 767.465088
     110.070946 442.277466
     114.110634 133.696457
     */


    private List<MGFScan> scans;
    private String mgfFileName;
    private List<String> usedScans;
    private IDMapping idMapping;

    Logger logger = LogManager.getFormatterLogger(this.getClass().getName());

    public List<MGFScan> getScans() {
        return scans;
    }

    public MGFReader(String fName) {
        this.mgfFileName = fName;
    }

    public void setIdMapping(IDMapping f) {this.idMapping = f;}
    public void setUsedScans(List<String> us) {
        this.usedScans = us;
    }

    public void clearScans() {
        logger.info("Clearing scans list");
        usedScans.clear();
        scans.clear();
    }

    @Override
    public void run() {
        boolean inScan = false;
        boolean storePeaks = false;
        int numberFormatErrors = 0;
        int numberStoredPeaks = 0;
        int numberIDScans = 0;
        MGFScan currentScan = null;

        this.scans = new ArrayList<>();

        logger.info("Begining to read file %s", this.mgfFileName);
        logger.info("%s is the Galaxy name, %s is the display name", this.mgfFileName, this.idMapping.scanFilesNameMap.get(this.mgfFileName));
        try (BufferedReader br = new BufferedReader(new FileReader(this.mgfFileName))) {
            String line;
            int scanCount = 0;
            while ((line = br.readLine()) != null) {
                if (line.matches("BEGIN IONS")) {
                    inScan = true;
                    currentScan = new MGFScan();
                    currentScan.sourceFile = this.idMapping.scanFilesNameMap.get(this.mgfFileName);
                    currentScan.scanIndex = scanCount;
                    scanCount++;
                }
                if (line.matches("END IONS")) {
                    inScan = false;
                    this.scans.add(currentScan);
                    currentScan = null;
                }
                if (line.matches("\\S+=\\S+")) {
                    String[] t = line.split("=");
                    if (inScan) {
                        CVPair cvp = new CVPair();
                        cvp.name = t[0];
                        cvp.value = t[1];
                        currentScan.metaData.add(cvp);
                        if (t[0].equals("TITLE")) {
                            if (this.usedScans.contains(t[1])) {
                                storePeaks = true;
                                numberIDScans++;
                            } else {
                                storePeaks = false;
                            }
                        }
                    }
                }
                if (line.matches("\\S+\\s\\S+")) {
                    if (inScan && storePeaks) {
                        try {
                            String[] t = line.split("\\s");
                            if (!t[0].contains("BEGIN")) {
                                FloatPair fp = new FloatPair();
                                fp.a = Float.valueOf(t[0]);
                                fp.b = Float.valueOf(t[1]);
                                currentScan.mzIntensities.add(fp);
                                numberStoredPeaks++;
                            }
                        } catch (NumberFormatException ne) {
                            numberFormatErrors++;
                        }
                    }
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Finished reading file %s", this.mgfFileName);
        logger.info("Parsed %,d scans", this.scans.size());
        logger.info("Stored %,d peaks from identified scans", numberStoredPeaks);
        logger.info("Stored %,d good scans ", numberIDScans);
        logger.info("Number format errors with peaks : %,d ", numberFormatErrors);
    }

}
