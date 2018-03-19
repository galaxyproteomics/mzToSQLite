package edu.umn.msi.gx.java;

import edu.umn.msi.gx.database.DatabaseManager;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.ext.DefaultHandler2;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SpectrumIdentificationHandler extends DefaultHandler2 implements SaxContentHandler {

    private Logger logger = Logger.getLogger(SpectrumIdentificationItem.class.getName());

    private Map<String, SpectrumIdentificationResult> spectrumIdentificationResults = new HashMap<>();
    private String spectrumID = null;
    private String spectraData_ref = null;
    private String resultID = null;

    /**
     * Contains the types of measures that will be reported in generic arrays for each SpectrumIdentificationItem e.g. product ion m/z, product ion intensity, product ion m/z error
     */
    private List<String> fragmentMeasures = new ArrayList<>();

    private SpectrumIdentificationItem currentSpectrumIdentificationItem;
    private IonType currentIonType;

    private int spectrumIDItemDepth = 0;

    private boolean inSpectrumIdentificationResult = false;
    private boolean inSpectrumIdentificationItem = false;
    private boolean inFragmentationTable = false;

    private String createSIIFragementSQL() {
        StringBuilder rVal = new StringBuilder();
        rVal.append("CREATE TABLE sii_fragments (sii_id TEXT, ");
        for (String m : fragmentMeasures) {
            rVal.append(m);
            rVal.append(" TEXT,");
        }
        rVal.deleteCharAt(rVal.lastIndexOf(","));
        rVal.append(")");
        logger.log(Level.INFO, rVal.toString());
        return rVal.toString();
    }

    private String createSIIFragmentInsert() {
        StringBuilder sb = new StringBuilder("INSERT INTO sii_fragments VALUES (?,");
        for (String m : fragmentMeasures) {
            sb.append("?,");
        }
        sb.deleteCharAt(sb.lastIndexOf(","));
        sb.append(")");
        logger.log(Level.INFO, sb.toString());
        return sb.toString();
    }

    public Map<String, SpectrumIdentificationResult> getSpectrumIdentificationResults() {
        return spectrumIdentificationResults;
    }

    public void generateSQL(DatabaseManager d) {
        logger.log(Level.INFO,"Creating tables in SpectrumIdentifiationHandler");
        Statement tblStmt;

        try {
            tblStmt = d.conn.createStatement();
            tblStmt.execute("CREATE TABLE spectrum_identification_results (id TEXT PRIMARY KEY, spectraData_ref TEXT, spectrumID TEXT, spectrumTitle TEXT);");
            tblStmt.execute("CREATE TABLE spectrum_identification_result_items (id TEXT PRIMARY KEY, spectrum_identification_result_ref TEXT, passThreshold TEXT, rank INTEGER, peptide_ref TEXT, calculatedMassToCharge FLOAT, experimentalMassToCharge FLOAT, chargeState INTEGER);");
            tblStmt.execute("CREATE TABLE sii_peptide_evidence (sii_id TEXT, peptide_evidence_ref TEXT)");
            tblStmt.execute("CREATE TABLE sii_scores (sii_id TEXT, score_name TEXT, score_value TEXT)");

            tblStmt.execute(this.createSIIFragementSQL());

            String sql1 = "INSERT INTO spectrum_identification_results VALUES (?,?,?,?)";
            String sql2 = "INSERT INTO spectrum_identification_result_items VALUES (?,?,?,?,?,?,?,?)";
            String sql3 = "INSERT INTO sii_scores VALUES (?,?,?)";
            String sql4 = "INSERT INTO sii_peptide_evidence VALUES (?,?)";

            String sql5 = this.createSIIFragmentInsert();

            PreparedStatement ps1 = d.conn.prepareStatement(sql1);
            PreparedStatement ps2 = d.conn.prepareStatement(sql2);
            PreparedStatement ps3 = d.conn.prepareStatement(sql3);
            PreparedStatement ps4 = d.conn.prepareStatement(sql4);
            PreparedStatement ps5 = d.conn.prepareStatement(sql5);

            for (String key : this.spectrumIdentificationResults.keySet()) {
                SpectrumIdentificationResult sr = this.spectrumIdentificationResults.get(key);
                ps1.setString(1, sr.id);
                ps1.setString(2, sr.spectraData_ref);
                ps1.setString(3, sr.spectrumID);
                ps1.setString(4, sr.spectrumTitle);
                ps1.addBatch();

                for (SpectrumIdentificationItem si : sr.items) {
                    ps2.setString(1, si.id);
                    ps2.setString(2, si.spectrumIdentificationResultID);

                    for (String pes : si.peptideEvidenceRef) {
                        ps4.setString(1, si.id);
                        ps4.setString(2, pes);
                        ps4.addBatch();
                    }

                    for (CVPair cp : si.params) {
                        if(cp.name.equals("passThreshold")) {
                            ps2.setString(3, cp.value);
                        }
                        if (cp.name.equals("rank")) {
                            ps2.setInt(4, Integer.valueOf(cp.value));
                        }
                        if (cp.name.equals("peptide_ref")) {
                            ps2.setString(5, cp.value);
                        }
                        if (cp.name.equals("calculatedMassToCharge")){
                            ps2.setFloat(6, Float.valueOf(cp.value));
                        }
                        if (cp.name.equals("experimentalMassToCharge")) {
                            ps2.setFloat(7, Float.valueOf(cp.value));
                        }
                        if (cp.name.equals("chargeState")) {
                            ps2.setInt(8, Integer.valueOf(cp.value));
                        }
                    }
                    ps2.addBatch();

                    //Ion run scores
                    for (IonType it : si.fragments) {
                        if (it.name.contains("iTRAQ")) {
                            continue;
                        }
                        Pattern p = Pattern.compile("[abyz] ion");
                        Matcher m = p.matcher(it.name);
                        if (m.find()) {
                            String sName = it.name + "+" + it.charge + "_consecutive_run";
                            ps3.setString(1, si.id);
                            ps3.setString(2, sName);
                            ps3.setString(3, String.valueOf(it.numConsecutive));
                            ps3.addBatch();
                        }
                        String s = it.name + "+" + it.charge + "_total_ions";
                        ps3.setString(1, si.id);
                        ps3.setString(2, s);
                        ps3.setString(3, String.valueOf(it.ionIndex.length));
                        ps3.addBatch();

                        //Get fragmentation values on this iontype;
                        ps5.setString(1, si.id);
                        int idx = 2;
                        for(String fm : fragmentMeasures) {
                            ps5.setString(idx, it.fragmentArray.get(fm));
                            idx++;
                        }
                        ps5.addBatch();
                    }

                    for (CVPair sp : si.scores) {
                        ps3.setString(1, si.id);
                        ps3.setString(2, sp.name);
                        ps3.setString(3, sp.value);
                        ps3.addBatch();
                    }
                }
            }

            int[] result = ps1.executeBatch();
            logger.log(Level.INFO, "Number of spectrum results inserted : {0}", result.length);
            ps1.clearParameters();
            ps1.close();
            result = ps2.executeBatch();
            logger.log(Level.INFO, "Number of spectrum result items inserted : {0}", result.length);
            ps2.clearParameters();
            ps2.close();
            result = ps3.executeBatch();
            logger.log(Level.INFO,"Number of spectrum result item scores inserted : {0}", result.length);
            ps3.clearParameters();
            ps3.close();
            result = ps4.executeBatch();
            logger.log(Level.INFO,"Number of spectrum result item peptide references inserted : {0}", result.length);
            ps4.clearParameters();
            ps4.close();
            result = ps5.executeBatch();
            logger.log(Level.INFO, "Added {0} fragment array values", result);
            ps5.clearParameters();
            ps5.close();
            //Index creation
            tblStmt.execute("CREATE INDEX FragmentsBySII ON sii_fragments (sii_id)");
            tblStmt.close();
            d.conn.commit();

        } catch (SQLException se) {
            se.printStackTrace();
        }
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes);

        switch (qName) {
            case "SpectrumIdentificationResult":
                inSpectrumIdentificationResult = true;
                spectraData_ref = attributes.getValue("spectraData_ref");
                spectrumID = attributes.getValue("spectrumID");
                resultID = attributes.getValue("id");

                this.spectrumIdentificationResults.put(resultID, new SpectrumIdentificationResult());
                this.spectrumIdentificationResults.get(resultID).id = resultID;
                this.spectrumIdentificationResults.get(resultID).spectraData_ref = spectraData_ref;
                this.spectrumIdentificationResults.get(resultID).spectrumID = spectrumID;
                break;
            case "SpectrumIdentificationItem":
                if (inSpectrumIdentificationResult) {
                    inSpectrumIdentificationItem = true;
                    spectrumIDItemDepth++;
                    this.currentSpectrumIdentificationItem = new SpectrumIdentificationItem();
                    this.currentSpectrumIdentificationItem.id = attributes.getValue("id");
                    this.currentSpectrumIdentificationItem.spectrumIdentificationResultID = resultID;

                    List<CVPair> lp = new ArrayList<>();
                    CVPair p;
                    for (int idx = 0; idx < attributes.getLength(); idx++) {
                        p = new CVPair();
                        p.name = attributes.getQName(idx);
                        p.value = attributes.getValue(idx);
                        lp.add(p);
                    }
                    this.currentSpectrumIdentificationItem.params = lp;

                }
                break;
            case "cvParam":
                if (inSpectrumIdentificationResult && spectrumIDItemDepth == 0) {
                    if (attributes.getValue("name").equals("spectrum title")) {
                        this.spectrumIdentificationResults.get(resultID).spectrumTitle = attributes.getValue("value");
                    }

                }
                if (spectrumIDItemDepth == 1) {
                    //Get scores related to MSMS
                    CVPair p = new CVPair();
                    p.name = attributes.getValue("name");
                    p.value = attributes.getValue("value");
                    this.currentSpectrumIdentificationItem.scores.add(p);
                }
                if (spectrumIDItemDepth == 3) {
                    //In ions, get the ion name
                    currentIonType.setName(attributes.getValue("name"));
                }

                break;
            case "FragmentationTable":
                inFragmentationTable = true; //TODO: do we want all cvparm entries??
                break;
            case "Measure":
                fragmentMeasures.add(attributes.getValue("id"));
                break;
            case "FragmentArray":
                currentIonType.fragmentArray.put(attributes.getValue("measure_ref"), attributes.getValue("values"));
                break;
            case "Fragmentation":
                spectrumIDItemDepth++;
                break;
            case "IonType":
                spectrumIDItemDepth++;
                currentIonType = new IonType();
                currentIonType.setCharge(attributes.getValue("charge"));
                currentIonType.calcIons(attributes.getValue("index"));
                break;
            case "PeptideEvidenceRef":
                if (inSpectrumIdentificationItem) {
                    this.currentSpectrumIdentificationItem.peptideEvidenceRef.add(attributes.getValue("peptideEvidence_ref"));
                }
                break;
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);
        switch (qName) {
            case "SpectrumIdentificationResult":
                inSpectrumIdentificationResult = false;
                spectrumID = null;
                break;
            case "SpectrumIdentificationItem":
                spectrumIDItemDepth = 0;
                inSpectrumIdentificationItem = false;
                this.spectrumIdentificationResults.get(resultID).items.add(this.currentSpectrumIdentificationItem);
                break;
            case "Fragmentation":
                spectrumIDItemDepth--;
                break;
            case "IonType":
                spectrumIDItemDepth--;
                this.currentSpectrumIdentificationItem.fragments.add(currentIonType);
                break;
            case "FragmentationTable":
                inFragmentationTable = false;
                break;
        }

    }
}
