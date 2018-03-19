package edu.umn.msi.gx.java;

import edu.umn.msi.gx.database.DatabaseManager;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.ext.DefaultHandler2;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public class AnalysisProtocolHandler extends DefaultHandler2 implements SaxContentHandler {

    class SearchModification {
        String residue;
        double massDelta;
        boolean fixedMod;
        String name;
        String specificityRule = "None";
    }

    class ModObj {
        String type; //fixed or variable
        String residue;
    }

    String analysisSoftwareName = null;
    String analysisSoftwareVersion = null;

    boolean inSpecificityRule = false;
    boolean inSearchModification = false;
    SearchModification currentSearchMod = null;

    Map<String, List<String>> proteinDetection = new HashMap<>();
    String currentProteinDetection;

    List<SearchModification> peptideModifications = new ArrayList<>();
    Map<Integer, List<ModObj>> modsByName = new HashMap<>();
    private Logger logger = Logger.getLogger(AnalysisProtocolHandler.class.getName());

    //The key must be name and residue.
    private void addModByName(SearchModification sm) {
        String kStr = sm.name + sm.residue + sm.specificityRule;
        int hc = kStr.hashCode();
        if (!(modsByName.containsKey(hc))) {
            modsByName.put(hc, new ArrayList<>());
        }
        ModObj mo = new ModObj();
        mo.residue = sm.residue;
        if (sm.fixedMod) { //TODO: make an enumeration
            mo.type = "fixed";
        } else {
            mo.type = "variable";
        }
        modsByName.get(hc).add(mo);
    };

    @Override
    public void generateSQL(DatabaseManager d) {
        logger.log(Level.INFO, "Creating tables for AnalysisProtocols.");

        List<String> aminoAcids = Arrays.asList("A","R","N","D","C","E","Q","G","H","I","L","K","M","F","P","S","T","W","Y","V");

        try {
            Statement stmt = d.conn.createStatement();

            stmt.execute("CREATE TABLE protein_detection_parms (id TEXT PRIMARY KEY , prot_detect_prot TEXT, spectrum_id_list TEXT)");
            PreparedStatement pps = d.conn.prepareStatement("INSERT INTO protein_detection_parms VALUES (?,?,?)");
            for (Map.Entry<String, List<String>> me : this.proteinDetection.entrySet()) {
                pps.setString(1, me.getKey());
                pps.setString(2, me.getValue().get(0));
                pps.setString(3, me.getValue().get(1));
                pps.addBatch();
            }
            pps.executeBatch();
            d.conn.commit();

            stmt.execute("CREATE TABLE search_modifications (id INTEGER PRIMARY KEY, name TEXT, fixedMod TEXT, massDelta FLOAT, residue TEXT, specificity TEXT)");
            String q = "INSERT INTO search_modifications VALUES (?,?,?,?,?,?)";
            PreparedStatement ps = d.conn.prepareStatement(q);
            int idx = 0;
            for(SearchModification s: peptideModifications) {
                if (s.residue.equals(".")) {
                    for (String aa : aminoAcids) {
                        ps.setInt(1, idx);
                        ps.setString(2, s.name);
                        ps.setString(3, String.valueOf(s.fixedMod));
                        ps.setDouble(4, s.massDelta);
                        ps.setString(5, aa);
                        ps.setString(6, s.specificityRule);
                        ps.addBatch();
                        idx++;
                        s.residue = aa;
                        addModByName(s);
                    }
                } else {
                    ps.setInt(1, idx);
                    ps.setString(2, s.name);
                    ps.setString(3, String.valueOf(s.fixedMod));
                    ps.setDouble(4, s.massDelta);
                    ps.setString(5, s.residue);
                    ps.setString(6, s.specificityRule);
                    ps.addBatch();
                    idx++;
                    addModByName(s);
                }
            }
            ps.executeBatch();
            d.conn.commit();
            logger.log(Level.INFO, "Inserted modification params");

            logger.log(Level.INFO, "Adding modification type to the peptide_modification table");
            ResultSet rs = stmt.executeQuery("SELECT PM.peptide_ref, PM.name, PM.residue, PM.location FROM peptide_modifications PM");
            PreparedStatement ps2 = d.conn.prepareStatement("UPDATE peptide_modifications SET modType=? WHERE peptide_ref=?");

            while (rs.next()) {
                String termLoc = "None";
                if (rs.getInt("location") == 0) { //TODO: add logic for cterm.
                    termLoc = "nterm";
                }
                String kStr = rs.getString("name") + rs.getString("residue") + termLoc;
                int hc = kStr.hashCode();
                List<ModObj> vals = modsByName.get(hc);
                for (ModObj mObj : vals) {
                    if (mObj.type.equals("fixed")) {
                        ps2.setString(1, "fixed");
                        ps2.setString(2, rs.getString("peptide_ref"));
                    } else {
                        ps2.setString(1, "variable");
                        ps2.setString(2, rs.getString("peptide_ref"));
                    }
                    ps2.addBatch();
                }

            }
            ps2.executeBatch();
            d.conn.commit();

            //New table
            if (this.analysisSoftwareName != null) {
                stmt.execute("CREATE TABLE analysis_software (name TEXT, version TEXT)");
                ps = d.conn.prepareStatement("INSERT  INTO analysis_software VALUES (?,?)");
                ps.setString(1,this.analysisSoftwareName);
                ps.setString(2,this.analysisSoftwareVersion);
                ps.addBatch();
                ps.executeBatch();
                d.conn.commit();
            }

            ps.close();
            ps2.close();
            pps.close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }



    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes);
        switch (qName) {
            case "AnalysisSoftware":
                this.analysisSoftwareName = attributes.getValue("name");
                this.analysisSoftwareVersion = attributes.getValue("version");
                break;
            case "ProteinDetection":
                this.proteinDetection.put(attributes.getValue("id"), new ArrayList<>());
                this.proteinDetection.get(attributes.getValue("id")).add(attributes.getValue("proteinDetectionProtocol_ref"));
                this.currentProteinDetection = attributes.getValue("id");
                break;
            case "InputSpectrumIdentifications":
                if (this.currentProteinDetection != null) {
                    this.proteinDetection.get(this.currentProteinDetection).add(attributes.getValue("spectrumIdentificationList_ref"));
                }
                break;
            case "SpecificityRules":
                inSpecificityRule = true;
                break;
            case "SearchModification":
                inSearchModification = true;
                currentSearchMod = new SearchModification();
                currentSearchMod.residue = attributes.getValue("residues");
                currentSearchMod.massDelta = Double.valueOf(attributes.getValue("massDelta"));
                currentSearchMod.fixedMod = Boolean.valueOf(attributes.getValue("fixedMod"));
                break;
            case "cvParam":
                if ((inSearchModification) && (!inSpecificityRule)) {
                    currentSearchMod.name = attributes.getValue("name");
                }
                if ((inSearchModification) && (inSpecificityRule)) {
                    if (attributes.getValue("name").contains("N-term")) {
                        currentSearchMod.specificityRule = "nterm";
                    } else {
                        currentSearchMod.specificityRule = "cterm";
                    }
                }
                break;
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);
        switch (qName) {
            case "ProteinDetection":
                this.currentProteinDetection = null;
                break;
            case "SpecificityRules":
                inSpecificityRule = false;
                break;
            case "SearchModification":
                inSearchModification = false;
                this.peptideModifications.add(currentSearchMod);
                currentSearchMod = null;
                break;
            case "cvParam":
                break;
        }
    }


}
