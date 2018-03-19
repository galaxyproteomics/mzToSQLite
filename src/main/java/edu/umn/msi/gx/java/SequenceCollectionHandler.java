package edu.umn.msi.gx.java;

import edu.umn.msi.gx.database.DatabaseManager;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.ext.DefaultHandler2;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SequenceCollectionHandler extends DefaultHandler2 implements SaxContentHandler {

    private Logger logger = LogManager.getLogger(SequenceCollectionHandler.class.getName());

    private List<DBSequence> sequenceCollection = new ArrayList<>();
    private Map<String, PeptideEvidence> peptideEvidence = new HashMap<>();
    private Map<String, Peptide> peptides = new HashMap<>();

    class PeptideEvidence {
        String id;
        String dBSequence_ref;
        boolean isDecoy;
        String pre;
        String post;
        int start;
        int end;
        String peptide_ref;
    }

    class DBSequence {
        String id;
        String accession;
        String searchDatabase_ref;
        String description;
        String sequence;
        int length;
    }

    private boolean inSequenceCollection = false;
    private boolean inDBSequence = false;
    private boolean inPeptideSequence = false;
    private boolean inModification = false;

    private DBSequence dbSeqObj = null;
    private Peptide currentPeptide = null;
    private Modification currentModification = null;

    public List<String> getDBSequenceIDs() {
        List<String> l = new ArrayList<>();
        for (DBSequence d : this.sequenceCollection) {
            l.add(d.id);
        }
        return l;
    }

    public void generateSQL(DatabaseManager d) {
        logger.info("Creating tables for SequenceCollectionHandler");
        Statement tblStmt;
        try {
            tblStmt = d.conn.createStatement();
            tblStmt.execute("CREATE TABLE peptide_evidence (id TEXT PRIMARY KEY, dBSequence_ref TEXT, isDecoy TEXT, pre TEXT, post TEXT, start INTEGER, end INTEGER, peptide_ref TEXT);");
            tblStmt.execute("CREATE TABLE protein_description (id TEXT PRIMARY KEY , accession TEXT, searchDatabase_ref TEXT, description TEXT);");
            tblStmt.execute("CREATE TABLE peptides (id TEXT PRIMARY KEY , sequence TEXT, encoded_sequence);");
            tblStmt.execute("CREATE TABLE peptide_modifications (peptide_ref TEXT, avgMassDelta FLOAT, location INTEGER, monoisotopicMassDelta FLOAT, residue TEXT, name TEXT, sequence TEXT, modType TEXT);");

            String sql1 = "INSERT INTO peptide_evidence VALUES (?,?,?,?,?,?,?,?)";
            String sql2 = "INSERT INTO protein_description VALUES (?,?,?,?)";
            String sql3 = "INSERT INTO peptides VALUES (?,?,?)";
            String sql4 = "INSERT INTO peptide_modifications VALUES (?,?,?,?,?,?,?,?)";

            PreparedStatement ps1 = d.conn.prepareStatement(sql1);
            PreparedStatement ps2 = d.conn.prepareStatement(sql2);
            PreparedStatement ps3 = d.conn.prepareStatement(sql3);
            PreparedStatement ps4 = d.conn.prepareStatement(sql4);

            for (String pK : this.peptides.keySet()) {
                Peptide pep = this.peptides.get(pK);
                ps3.setString(1, pep.id);
                ps3.setString(2, pep.sequence);
                ps3.setNull(3, Types.NULL);
                ps3.addBatch();

                for (Modification m : pep.modifications) {
                    ps4.setString(1, pep.id);
                    ps4.setDouble(2, m.avgMassDelta);
                    ps4.setInt(3, m.location);
                    ps4.setDouble(4, m.monoisotopicMassDelta);
                    ps4.setString(5, m.residues);
                    ps4.setString(6, m.name);
                    ps4.setString(7, pep.sequence);
                    ps4.setNull(8, Types.NULL);
                    ps4.addBatch();
                }
            }

            for (String k : this.peptideEvidence.keySet()) {
                PeptideEvidence tmpPE = this.peptideEvidence.get(k);
                ps1.setString(1, tmpPE.id);
                ps1.setString(2, tmpPE.dBSequence_ref);
                ps1.setString(3, String.valueOf(tmpPE.isDecoy));
                ps1.setString(4, tmpPE.pre);
                ps1.setString(5, tmpPE.post);
                ps1.setInt(6, tmpPE.start);
                ps1.setInt(7, tmpPE.end);
                ps1.setString(8, tmpPE.peptide_ref);
                ps1.addBatch();
            }

            for (DBSequence dbs : this.sequenceCollection) {
                ps2.setString(1, dbs.id);
                ps2.setString(2, dbs.accession);
                ps2.setString(3, dbs.searchDatabase_ref);
                ps2.setString(4, dbs.description);
                ps2.addBatch();
            }

            int[] result = ps1.executeBatch();
            logger.info( "Number of peptide_evidence inserts : {}", result.length);
            ps1.clearParameters();
            ps1.close();
            result = ps2.executeBatch();
            logger.info("Number of DBSequence inserts : {}", result.length);
            ps2.clearParameters();
            ps2.close();
            result = ps3.executeBatch();
            logger.info("Number of peptide inserts : {}", result.length);
            ps3.clearParameters();
            ps3.close();
            result = ps4.executeBatch();
            logger.info("Number of peptide modifications inserts : {}", result.length);
            ps4.clearParameters();
            ps4.close();

            logger.info( "Creating INDEX ModsByPeptideRef");
            tblStmt.executeUpdate("CREATE INDEX ModsByPeptideRef ON peptide_modifications(peptide_ref)");
            logger.info( "Creating INDEX PeptEvidenceByPepRef");
            tblStmt.executeUpdate("CREATE INDEX PeptEvidenceByPepRef ON peptide_evidence(peptide_ref)");
            logger.info( "Creating INDEX pepEvidenceByProteinRef");
            tblStmt.executeUpdate("CREATE INDEX pepEvidenceByProteinRef ON peptide_evidence(dBSequence_ref)");
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
            case "PeptideEvidence":
                PeptideEvidence pe = new PeptideEvidence();
                pe.id = attributes.getValue("id");
                pe.dBSequence_ref = attributes.getValue("dBSequence_ref");
                pe.isDecoy = Boolean.valueOf(attributes.getValue("isDecoy"));
                pe.pre = attributes.getValue("pre");
                pe.post = attributes.getValue("post");
                pe.start = Integer.valueOf(attributes.getValue("start"));
                pe.end = Integer.valueOf(attributes.getValue("end"));
                pe.peptide_ref = attributes.getValue("peptide_ref");
                this.peptideEvidence.put(attributes.getValue("id"), pe);
                break;
            case "SequenceCollection":
                inSequenceCollection = true;
                break;
            case "DBSequence":
                inDBSequence = true;
                dbSeqObj = new DBSequence();
                dbSeqObj.id = attributes.getValue("id");
                dbSeqObj.accession = attributes.getValue("accession");
                dbSeqObj.searchDatabase_ref = attributes.getValue("searchDatabase_ref");
                break;
            case "Peptide":
                currentPeptide = new Peptide();
                currentPeptide.id = attributes.getValue("id");
                break;
            case "PeptideSequence":
                inPeptideSequence = true;
                break;
            case "Modification":
                inModification = true;
                Modification m = new Modification();
                if (attributes.getValue("avgMassDelta") != null) {
                    m.avgMassDelta = Double.valueOf(attributes.getValue("avgMassDelta"));
                }
                if (attributes.getValue("monoisotopicMassDelta") != null) {
                    m.monoisotopicMassDelta = Double.valueOf(attributes.getValue("monoisotopicMassDelta"));
                }
                if (attributes.getValue("location") != null) {
                    m.location = Integer.valueOf(attributes.getValue("location"));
                }
                if (attributes.getValue("residues") != null) {
                    m.residues = attributes.getValue("residues");
                }
                this.currentModification = m;
                break;
            case "cvParam":
                if (inModification) {
                    this.currentModification.name = attributes.getValue("name");
                }
                if (inDBSequence) {
                    if (attributes.getValue("name").equals("protein description")) {
                        dbSeqObj.description = attributes.getValue("value");
                    }
                    if (attributes.getValue("name").equals("protein sequence")) {
                        dbSeqObj.sequence = attributes.getValue("value");
                    }
                }
                break;
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);

        switch (qName) {
            case "SequenceCollection":
                inSequenceCollection = false;
                break;
            case "DBSequence":
                inDBSequence = false;
                sequenceCollection.add(dbSeqObj);
                break;
            case "Peptide":
                this.peptides.put(currentPeptide.id, currentPeptide);
                currentPeptide = null;
                break;
            case "PeptideSequence":
                inPeptideSequence = false;
                break;
            case "Modification":
                inModification = false;
                this.currentPeptide.modifications.add(this.currentModification);
                currentModification = null;
                break;
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        super.characters(ch, start, length);
        if (inPeptideSequence) {
            currentPeptide.sequence = new String(ch, start, length);
        }
    }
}
