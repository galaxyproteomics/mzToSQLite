package edu.umn.msi.gx.java;

import edu.umn.msi.gx.database.DatabaseManager;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.ext.DefaultHandler2;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProteinAmbiguityHandler extends DefaultHandler2 implements SaxContentHandler {

    private Logger logger = LogManager.getLogger(ProteinAmbiguityHandler.class.getName());

    class PeptideHypothesis {
        String peptideEvidenceRef;
        String proteinDetectionHypothesisRef;
        List<String> spectrumIdentificationItems = new ArrayList<>();
    }

    private Map<String, List<CVPair>> proteinAmbiguityGroup = new HashMap<>();
    private Map<String, List<CVPair>> proteinDetectionHypothesis = new HashMap<>();
    private Map<String, PeptideHypothesis> peptideHypothesis = new HashMap<>();

    private boolean inProteinAmbiguityGroup = false;
    private boolean inProteinDetectionHypothesis = false;
    private boolean inPeptideHypothesis = false;
    private boolean inThreshold = false;

    private String proteinAmbiguityGroupID = null;
    private String proteinDetectionHypothesisID = null;
    private Map<String, CVPair> proteinDetectionProtocol = new HashMap<>();

    private String protDetProtID = null;
    private PeptideHypothesis currentPeptideHypothesis = null;

    public void generateSQL(DatabaseManager d) {
        logger.info( "Generating tables and inserting data.");
        Statement tblStmt;
        try {
            tblStmt = d.conn.createStatement();
            tblStmt.execute("CREATE TABLE protein_ambiguity_group (pag_value TEXT PRIMARY KEY);");
            tblStmt.execute("CREATE TABLE protein_ambiguity_group_parms (pag_value_ref TEXT, cvparm_name TEXT, cvparm_value TEXT);");
            tblStmt.execute("CREATE TABLE protein_detection_hypothesis (id TEXT PRIMARY KEY, pag_value_ref TEXT, sequence_coverage FLOAT, dbSequence_ref TEXT);");
            tblStmt.execute("CREATE TABLE peptide_hypothesis (id INTEGER PRIMARY KEY, peptideEvidence_ref TEXT, protein_detection_hyp_ref TEXT);");
            tblStmt.execute("CREATE TABLE peptide_hypothesis_parms (hypothesis_id_ref INTEGER, spectrum_id_item_ref TEXT);");
            tblStmt.execute("CREATE TABLE protein_detection_protocol (id TEXT PRIMARY KEY , name TEXT, value TEXT);");
            tblStmt.close();

            String sqlQ1 = "INSERT INTO protein_ambiguity_group VALUES (?)";
            String sqlQ2 = "INSERT INTO protein_ambiguity_group_parms VALUES (?,?,?)";
            String sqlQ3 = "INSERT INTO protein_detection_hypothesis VALUES (?, ?, ?, ?)";
            String sqlQ4 = "INSERT INTO peptide_hypothesis VALUES (?, ?, ?)";
            String sqlQ5 = "INSERT INTO peptide_hypothesis_parms VALUES (?, ?)";
            String sqlQ6 = "INSERT INTO protein_detection_protocol VALUES (?,?,?)";

            PreparedStatement pStmt1 = d.conn.prepareStatement(sqlQ1);
            PreparedStatement pStmt2 = d.conn.prepareStatement(sqlQ2);
            PreparedStatement pStmt3 = d.conn.prepareStatement(sqlQ3);
            PreparedStatement pStmt4 = d.conn.prepareStatement(sqlQ4);
            PreparedStatement pStmt5 = d.conn.prepareStatement(sqlQ5);
            PreparedStatement pStmt6 = d.conn.prepareStatement(sqlQ6);

            Iterator itr = this.proteinAmbiguityGroup.entrySet().iterator();
            Map.Entry pair;
            while (itr.hasNext()) {
                pair = (Map.Entry)itr.next();
                pStmt1.setString(1, String.valueOf(pair.getKey()));
                pStmt1.addBatch();
                for (CVPair cp : (ArrayList<CVPair>)pair.getValue()) {
                    pStmt2.setString(1, String.valueOf(pair.getKey()));
                    pStmt2.setString(2, cp.name);
                    pStmt2.setString(3, cp.value);
                    pStmt2.addBatch();
                }
            }

            itr = this.proteinDetectionHypothesis.entrySet().iterator();
            while (itr.hasNext()) {
                pair = (Map.Entry)itr.next();
                pStmt3.setString(1, String.valueOf(pair.getKey()));
                for (CVPair cp : (ArrayList<CVPair>)pair.getValue()) {
                    if (cp.name.equals("proteinAmbiguityGroupID")) {
                        pStmt3.setString(2, cp.value);
                    }
                    if (cp.name.equals("sequence coverage")) {
                        pStmt3.setString(3, cp.value);
                    }
                    if(cp.name.equals("dbSequence_ref")) {
                        pStmt3.setString(4, cp.value);
                    }
                }
                pStmt3.addBatch();
            }

            int insert_idx = 0;
            for (String k : this.peptideHypothesis.keySet()) {
                PeptideHypothesis tmpP = this.peptideHypothesis.get(k);
                pStmt4.setInt(1, insert_idx);
                pStmt4.setString(2, tmpP.peptideEvidenceRef);
                pStmt4.setString(3, tmpP.proteinDetectionHypothesisRef);
                pStmt4.addBatch();
                for (String sidItem : tmpP.spectrumIdentificationItems) {
                    pStmt5.setInt(1, insert_idx);
                    pStmt5.setString(2, sidItem);
                    pStmt5.addBatch();
                }
                insert_idx++;
            }

            //   tblStmt.execute("CREATE TABLE protein_detection_protocol (id TEXT PRIMARY KEY , name TEXT, value TEXT);
            for (Map.Entry<String, CVPair> me : this.proteinDetectionProtocol.entrySet()) {
                pStmt6.setString(1, me.getKey());
                pStmt6.setString(2, me.getValue().name);
                pStmt6.setString(3, me.getValue().value);
                pStmt6.addBatch();
            }


            int[] result = pStmt1.executeBatch();
            logger.info( "Number of PAG inserted : {}", result.length);
            pStmt1.clearParameters();
            pStmt1.close();
            result = pStmt2.executeBatch();
            logger.info( "Number of PAG parms inserted : {}", result.length);
            pStmt2.clearParameters();
            pStmt2.close();
            result = pStmt3.executeBatch();
            logger.info( "Number of proteinDetectionHyptohesis added : {}", result.length);
            pStmt3.clearParameters();
            pStmt3.close();
            result = pStmt4.executeBatch();
            logger.info( "Number of peptide hypotheis added : {}", result.length);
            pStmt4.clearParameters();
            pStmt4.close();
            result = pStmt5.executeBatch();
            logger.info( "Number of peptide hypothesis parms added: {}", result.length);
            pStmt5.clearParameters();
            pStmt5.close();
            pStmt6.executeBatch();
            pStmt6.clearParameters();
            pStmt6.close();

            d.conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes);
        List<CVPair> lp = null;
        switch (qName) {
            case "ProteinDetectionProtocol":
                this.protDetProtID = attributes.getValue("id");
                break;
            case "Threshold":
                this.inThreshold = true;
                break;
            case "ProteinAmbiguityGroup":
                inProteinAmbiguityGroup = true;
                proteinAmbiguityGroupID = attributes.getValue("id");
                lp = new ArrayList<>();
                proteinAmbiguityGroup.put(proteinAmbiguityGroupID, lp);
                break;
            case "ProteinDetectionHypothesis":
                inProteinDetectionHypothesis = true;
                proteinDetectionHypothesisID = attributes.getValue("id");
                lp = new ArrayList<>();
                proteinDetectionHypothesis.put(proteinDetectionHypothesisID, lp);
                //Link protein ID to the protein group
                CVPair p1 = new CVPair();
                p1.name = "proteinAmbiguityGroupID";
                p1.value = proteinAmbiguityGroupID;
                proteinDetectionHypothesis.get(proteinDetectionHypothesisID).add(p1);
                p1 = new CVPair();
                p1.name = "dbSequence_ref";
                p1.value = attributes.getValue("dBSequence_ref");
                proteinDetectionHypothesis.get(proteinDetectionHypothesisID).add(p1);
                break;
            case "PeptideHypothesis":
                inPeptideHypothesis = true;
                PeptideHypothesis ph = new PeptideHypothesis();
                ph.peptideEvidenceRef = attributes.getValue("peptideEvidence_ref");
                ph.proteinDetectionHypothesisRef = proteinDetectionHypothesisID;
                this.currentPeptideHypothesis = ph;
                peptideHypothesis.put(attributes.getValue("peptideEvidence_ref"), ph);
                break;
            case "SpectrumIdentificationItemRef":
                this.currentPeptideHypothesis.spectrumIdentificationItems.add(attributes.getValue("spectrumIdentificationItem_ref"));
                break;
            case "cvParam":
                CVPair p = null;
                if (inThreshold && this.protDetProtID != null) {
                    p = new CVPair();
                    p.name = attributes.getValue("name");
                    p.value = attributes.getValue("value");
                    this.proteinDetectionProtocol.put(this.protDetProtID, p);
                }
                if (inProteinAmbiguityGroup) {
                    p = new CVPair();
                    p.name = attributes.getValue("name");
                    p.value = attributes.getValue("value");
                }

                if (inProteinDetectionHypothesis) {
                    proteinDetectionHypothesis.get(proteinDetectionHypothesisID).add(p);
                }

                if (inProteinAmbiguityGroup && !inProteinDetectionHypothesis) {
                    proteinAmbiguityGroup.get(proteinAmbiguityGroupID).add(p);
                }
                break;
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);

        switch (qName) {
            case "Threshold":
                inThreshold = false;
                this.protDetProtID = null;
                break;
            case "ProteinAmbiguityGroup":
                inProteinAmbiguityGroup = false;
                break;
            case "ProteinDetectionHypothesis":
                inProteinDetectionHypothesis = false;
                break;
            case "PeptideHypothesis":
                inPeptideHypothesis = false;
                break;
            case "SpectrumIdentificationItemRef":
                break;
        }
    }
}
