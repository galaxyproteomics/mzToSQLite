package edu.umn.msi.gx.database;

import edu.umn.msi.gx.java.Score;
import edu.umn.msi.gx.java.SpectrumIdentificationResult;
import edu.umn.msi.gx.mgf.MGFScan;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.stat.StatUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MetaTableManager {

    private static Logger logger = LogManager.getFormatterLogger(MetaTableManager.class.getName());
    private static DatabaseManager dbMgr;

    public static void setDatabaseManager(DatabaseManager d) {
        MetaTableManager.dbMgr = d;
    }

    //SQL update to peptides table creating a new column - encoded_sequence - and adding values
    private static void addEncodedSequences(Map<String, String> es) {
        String q = "UPDATE peptides SET encoded_sequence=? WHERE id=?";

        try {
            PreparedStatement ps = MetaTableManager.dbMgr.conn.prepareStatement(q);

            for (Map.Entry<String, String> entry: es.entrySet()) {
                ps.setString(1, entry.getValue());
                ps.setString(2, entry.getKey());
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
            MetaTableManager.dbMgr.conn.commit();
            logger.info("All encoded sequences added to peptides table");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /*
    Creates modification encoded seqeunces to the peptides table, this makes the
    visual presentation of peptide modificatiouns faster.
     */
    public static void createEncodedSequences() {
        logger.info("Adding modification sequence encoding to peptides table");

        class Mod {
            int location;
            String name;
            String sequence;
            String residue;

            public void setLocation(int l) {
                location = l;
            }
        }

        Map<String, List<Mod>> modsBySequence = new HashMap<>();
        Map<String, String> encodedSequences = new HashMap<>();
        String q = "SELECT PM.peptide_ref, PM.location, PM.name, PM.sequence, PM.residue FROM peptide_modifications PM ORDER BY PM.peptide_ref";

        try {
            Statement stmt = MetaTableManager.dbMgr.conn.createStatement();
            ResultSet rs = stmt.executeQuery(q);
            while (rs.next()) {
                Mod m = new Mod();
                m.setLocation(rs.getInt("location"));
                m.name = rs.getString("name");
                m.residue = rs.getString("residue");
                m.sequence = rs.getString("sequence");
                if (!(modsBySequence.containsKey(rs.getString("peptide_ref")))) {
                    modsBySequence.put(rs.getString("peptide_ref"), new ArrayList<Mod>());
                }
                modsBySequence.get(rs.getString("peptide_ref")).add(m);
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        for (Map.Entry<String, List<Mod>> entry : modsBySequence.entrySet()) {
            String pepRef = entry.getKey();
            Map<Integer, List<String>> mByPos = new HashMap<>();
            char[] rawSequence = entry.getValue().get(0).sequence.toCharArray();

            for (Mod mod : entry.getValue()) {
                if (!(mByPos.containsKey(mod.location))) {
                    mByPos.put(mod.location, new ArrayList<String>());
                }
                mByPos.get(mod.location).add(mod.name);
            }

            StringBuilder encodedSequence = new StringBuilder();

            for (int idx = 0; idx < rawSequence.length; idx++) {
                //N-term mod
                if ((idx == 0) && (mByPos.containsKey(0))) {
                    encodedSequence.append('[');
                    for (String mName : mByPos.get(idx)) {
                        encodedSequence.append(mName);
                        encodedSequence.append(',');
                    }
                    encodedSequence.deleteCharAt(encodedSequence.lastIndexOf(","));
                    encodedSequence.append(']');
                    encodedSequence.append(rawSequence[idx]);
                } else {
                    //side chain mod.
                    if (mByPos.containsKey(idx + 1)) {
                        encodedSequence.append('{');
                        encodedSequence.append(rawSequence[idx]);
                        encodedSequence.append(':');
                        for (String mName : mByPos.get(idx + 1)) {
                            encodedSequence.append(mName);
                            encodedSequence.append(',');
                        }
                        encodedSequence.deleteCharAt(encodedSequence.lastIndexOf(","));
                        encodedSequence.append('}');
                    } else {
                        encodedSequence.append(rawSequence[idx]);
                    }
                }
            }
            //C-term mod
            if (mByPos.containsKey(rawSequence.length + 1)) {
                encodedSequence.append('[');
                for (String mName : mByPos.get(rawSequence.length + 1)) {
                    encodedSequence.append(mName);
                    encodedSequence.append(',');
                }
                encodedSequence.deleteCharAt(encodedSequence.lastIndexOf(","));
                encodedSequence.append(']');
            }
            encodedSequences.put(pepRef, encodedSequence.toString());
        }
        logger.info("Sequences are encoded.");
        MetaTableManager.addEncodedSequences(encodedSequences);
    }

    public static void createPepToProteinTable() {
        logger.info("Creating the peptide to protein table.");

        try {
            Statement stmt = MetaTableManager.dbMgr.conn.createStatement();
            logger.info("Build db_sequence TABLE based on protein_description and protein_sequence");
            stmt.execute("CREATE TABLE db_sequence AS " +
                    " SELECT PD.id, PD.accession, PD.searchDatabase_ref, PD.description, PS.sequence, PS.length " +
                    " FROM protein_description PD LEFT JOIN protein_sequence PS ON PD.id = PS.id");
            stmt.execute("CREATE INDEX fooBar ON db_sequence(id)");
            MetaTableManager.dbMgr.conn.commit();


            logger.info("DROP tables protein_description and protein_sequence");
            stmt.execute("DROP TABLE protein_sequence;");
            stmt.execute("DROP TABLE protein_description;");
            MetaTableManager.dbMgr.conn.commit();

            int r = stmt.executeUpdate("CREATE TABLE proteins_by_peptide AS \n" +
                    " SELECT peptide_evidence.peptide_ref,\n" +
                    "  db_sequence.*\n" +
                    "FROM peptide_evidence, db_sequence\n" +
                    "  WHERE db_sequence.id = peptide_evidence.dBSequence_ref\n" +
                    "ORDER BY peptide_ref");

            logger.info("Ran update query with result {}", r);
            stmt.executeUpdate("CREATE INDEX protByPep ON proteins_by_peptide(peptide_ref)");
            stmt.close();
            MetaTableManager.dbMgr.conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createPSMTable() {
        logger.info("Creating the PSM meta table.");
        Statement stmt;
        Map<String, String> scoreTypes = new HashMap<>();

        try {
            stmt = MetaTableManager.dbMgr.conn.createStatement();
            String sQ = "SELECT score_summary.score_name, score_summary.score_type FROM score_summary";
            ResultSet rs = stmt.executeQuery(sQ);
            while (rs.next()) {
                scoreTypes.put(rs.getString("score_name"), rs.getString("score_type"));
            }
            rs.close();

            //Build query for PSM
            String cQ = "CREATE TABLE psm_entries AS \n" +
                    " SELECT peptides.id, peptides.sequence, sii.passThreshold, sir.spectrumID, sir.spectrumTitle, \n" +
                    " ##SCORES## \n" +
                    " FROM\n" +
                    "  spectrum_identification_result_items sii,\n" +
                    "  spectrum_identification_results sir,\n" +
                    "  peptides,\n" +
                    "  sii_scores \n" +
                    "WHERE\n" +
                    "  sii.peptide_ref = peptides.id AND\n" +
                    "  sir.id = sii.spectrum_identification_result_ref AND\n" +
                    "  sii_scores.sii_id = sii.id \n" +
                    " GROUP BY sir.spectrumID, sir.spectrumTitle, peptides.id, peptides.sequence ";
            StringBuilder sb = new StringBuilder();
            for (String s : scoreTypes.keySet()) {
                // MAX(CASE WHEN sii_scores.score_name = "Comet:expectation value" THEN CAST(sii_scores.score_value AS REAL) END) AS "Comet",
                sb.append("MAX(CASE WHEN sii_scores.score_name = ");
                sb.append('"');
                sb.append(s);
                sb.append('"');
                if (scoreTypes.get(s).matches("REAL")) {
                    sb.append(" THEN CAST(sii_scores.score_value AS REAL) END) AS ");
                } else {
                    sb.append(" THEN sii_scores.score_value END) AS ");
                }
                sb.append('"');
                sb.append(s);
                sb.append('"');
                sb.append(',');
            }
            sb.deleteCharAt(sb.lastIndexOf(","));
            cQ = cQ.replace("##SCORES##", sb.toString());
            logger.info("PSM Query : {}", cQ);
            int r = stmt.executeUpdate(cQ);
            logger.info("Create PSM table with code {}", r);

            logger.info("Creating INDEX PSMBySeq");
            //CREATE INDEX PSMBySeq ON psm_entries(sequence)
            stmt.executeUpdate("CREATE INDEX PSMBySeq ON psm_entries(sequence)");
            stmt.executeUpdate("CREATE INDEX PSMByPepRef ON psm_entries(id)");
            stmt.close();
            MetaTableManager.dbMgr.conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create a table with sequences and length. References the db_seqeunce table on id
     * @param s
     */
    public static void addProteinSequences(Map<String, String> s) {
        logger.info("Creating protein_sequence table");

        try {
            Statement stmt = MetaTableManager.dbMgr.conn.createStatement();
            stmt.execute("CREATE TABLE protein_sequence (id TEXT PRIMARY KEY, sequence TEXT, length INTEGER)");

            String sql = "INSERT INTO protein_sequence VALUES (?,?,?)";
            PreparedStatement ps = dbMgr.conn.prepareStatement(sql);

            for (Map.Entry entry: s.entrySet()) {
                ps.setString(1, (String)entry.getKey());
                ps.setString(2, (String)entry.getValue());
                ps.setInt(3, ((String) entry.getValue()).length());
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
            stmt.close();
            MetaTableManager.dbMgr.conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        logger.info("Created protein_sequence table. With %,d rows", s.size());
    }

    /**
     * Determine the type of each distinct score in sii_scores.
     * <p>
     * In addition, generate basic statistics about each score.
     */
    public static void generateScoreSummaryTable(int numberOfIDResults) {
        logger.info("Creating score summary table");
        logger.info("There are %,d scores possible for each score name", numberOfIDResults);
        Map<String, List<String>> allScores = new HashMap<>();

        try {
            Statement stmt = MetaTableManager.dbMgr.conn.createStatement();
            String tblCreate = "CREATE TABLE score_summary (score_name TEXT PRIMARY KEY, score_type TEXT, number_values INTEGER, min_value REAL, max_value REAL, mean_value REAL, variance_value REAL, pct_scores_present REAL)";
            stmt.execute(tblCreate);

            String selQ = "SELECT sii_scores.score_name, sii_scores.score_value FROM sii_scores";
            ResultSet rs = stmt.executeQuery(selQ);
            while (rs.next()) {
                if (!allScores.containsKey(rs.getString("score_name"))) {
                    allScores.put(rs.getString("score_name"), new ArrayList<>());
                }
                allScores.get(rs.getString("score_name")).add(rs.getString("score_value"));
            }
            rs.close();
            stmt.close();

            String insQ = "INSERT INTO score_summary VALUES (?,?,?,?,?,?,?,?)";
            PreparedStatement ps = MetaTableManager.dbMgr.conn.prepareStatement(insQ);
            List<Double> isNumber;
            for (String sName : allScores.keySet()) {
                isNumber = allScores.get(sName).stream()
                        .map(sv -> {
                            if (NumberUtils.isCreatable(sv)) {
                                return NumberUtils.createDouble(sv);
                            } else {
                                return 0d;
                            }
                        })
                        .collect(Collectors.toList());

                double[] a = isNumber.stream().mapToDouble(i -> i).toArray();
                double sum = StatUtils.sum(a);

                ps.setString(1, sName);
                if (sum > 0) {

                    ps.setString(2, "REAL");
                    ps.setDouble(4, StatUtils.min(a));
                    ps.setDouble(5, StatUtils.max(a));
                    ps.setDouble(6, StatUtils.mean(a));
                    ps.setDouble(7, StatUtils.variance(a));

                } else {
                    ps.setString(2, "TEXT");
                    ps.setNull(4, Types.DOUBLE);
                    ps.setNull(5, Types.DOUBLE);
                    ps.setNull(6, Types.DOUBLE);
                    ps.setNull(7, Types.DOUBLE);
                }
                ps.setInt(3, allScores.get(sName).size());
                ps.setDouble(8, (double)allScores.get(sName).size() / (double)numberOfIDResults);
                ps.addBatch();
            }
            ps.executeBatch();
            ps.close();
            MetaTableManager.dbMgr.conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void addEnhancedFragmentScores(Map<String, SpectrumIdentificationResult> spectra) {
        logger.info("Adding enhanced fragment scores to sii_score");

        //Get sequence so I can calculate total possible number of fragment peaks per sprectrum.
        Map<String, String> seqBySII = new HashMap<>();
        Map<String, List<Score>> ionScores = new HashMap<>();

        String scoreQuery = "SELECT sii_scores.sii_id, sii_scores.score_name, sii_scores.score_value FROM sii_scores\n" +
                "WHERE sii_scores.score_name LIKE '%total_ions'";
        String q = "SELECT sii_peptide_evidence.sii_id, peptides.sequence\n" +
                "FROM sii_peptide_evidence, peptide_evidence, peptides\n" +
                "WHERE sii_peptide_evidence.peptide_evidence_ref = peptide_evidence.id AND\n" +
                "  peptides.id = peptide_evidence.peptide_ref";

        try {
            Statement stmt = MetaTableManager.dbMgr.conn.createStatement();
            ResultSet rs = stmt.executeQuery(q);
            while (rs.next()) {
                seqBySII.put(rs.getString("sii_id"), rs.getString("sequence"));
            }
            rs.close();

            rs = stmt.executeQuery(scoreQuery);
            while (rs.next()) {
                Pattern p = Pattern.compile("[abcxyz] ion");
                Matcher m = p.matcher(rs.getString("score_name"));

                if (m.find()) {
                    if (!ionScores.containsKey(rs.getString("sii_id"))) {
                        ionScores.put(rs.getString("sii_id"),
                                new ArrayList<>());
                    }
                    Score s = new Score();
                    s.name =rs.getString("score_name");
                    s.value = rs.getString("score_value");
                    ionScores.get(rs.getString("sii_id")).add(s);
                }
            }
            rs.close();
            stmt.close();

            String insrtQ = "INSERT INTO sii_scores VALUES (?,?,?)";
            PreparedStatement pstmt = MetaTableManager.dbMgr.conn.prepareStatement(insrtQ);

            for (String sii_id : seqBySII.keySet()) {
                List<Score> scores = ionScores.get(sii_id);
                if (scores != null) { //May have some spectra with no scores.
                    for (Score sp : scores) {
                        String newScoreName = sp.name.replace("total_ions", "pct_peaks_matched");
                        int possiblePeaks = seqBySII.get(sii_id).length();
                        Float newScoreValue = Float.valueOf(sp.value) / possiblePeaks;
                        pstmt.setString(1, sii_id);
                        pstmt.setString(2, newScoreName);
                        pstmt.setString(3, String.valueOf(newScoreValue));
                        pstmt.addBatch();
                    }
                }
            }

            int[] r = pstmt.executeBatch();
            logger.info("Inserted %,d new pct_peaks_matched scores", r.length);
            pstmt.clearParameters();
            pstmt.close();
            MetaTableManager.dbMgr.conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    public static void addMGFScores() {
        String sQ = "SELECT spectrum_identification_result_items.id, scans.calculatedTIC\n" +
                "FROM spectrum_identification_results, scans, spectrum_identification_result_items\n" +
                "WHERE spectrum_identification_results.spectrumTitle = scans.spectrumTitle AND\n" +
                "      spectrum_identification_results.spectrumID = scans.spectrumID AND\n" +
                "      spectrum_identification_result_items.spectrum_identification_result_ref = spectrum_identification_results.id";

        String uQ = "INSERT INTO sii_scores VALUES (?,?,?)";

        logger.info("Begin adding TIC to sii_score table.");

        try {
            Statement stmt = MetaTableManager.dbMgr.conn.createStatement();
            PreparedStatement ps = MetaTableManager.dbMgr.conn.prepareStatement(uQ);

            ResultSet rs = stmt.executeQuery(sQ);

            while (rs.next()) {
                ps.setString(1, rs.getString("id"));
                ps.setString(2, "tic");
                ps.setFloat(3, rs.getFloat("calculatedTIC"));
                ps.addBatch();
            }
            rs.close();
            stmt.close();
            int[] r = ps.executeBatch();
            logger.info("Update sii_score with %,d TIC scores.", r.length);
            ps.clearParameters();
            ps.close();
            MetaTableManager.dbMgr.conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void setMGFData(List<MGFScan> scans) {
        logger.info("If needed, creating peaks and scans table.");
        String q1 = "CREATE TABLE IF NOT EXISTS scans (spectrumID TEXT, spectrumTitle TEXT, sourceFile TEXT, calculatedTIC FLOAT, mzValues TEXT, intensities TEXT, PRIMARY KEY (spectrumID, spectrumTitle));";

        try {
            Statement stmt = MetaTableManager.dbMgr.conn.createStatement();
            int result = stmt.executeUpdate(q1);
            logger.info("scans creation result %d", result);
            logger.info("Inserting data");
            PreparedStatement ps1 = MetaTableManager.dbMgr.conn.prepareStatement("INSERT INTO scans VALUES (?,?,?,?,?,?)");
            for (MGFScan s : scans) {
                ps1.setString(1, s.getSpectrumID());
                ps1.setString(2, s.getSpectrumTitle());
                ps1.setString(3, s.getSourceFile());
                ps1.setFloat(4, s.getTIC());
                ps1.setString(5, s.getMZValueList());
                ps1.setString(6, s.getIntensityValueList());
                ps1.addBatch();
            }
            int[] r = ps1.executeBatch();
            logger.info("Scan insert finished with count %,d", r.length);
            ps1.close();
            stmt.close();
            MetaTableManager.dbMgr.conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createCountsTable() {
        try {
            logger.info("Creating meta table spectrum_counts");
            String q = "CREATE TABLE spectrum_counts AS \n " +
                    "SELECT\n" +
                    "  spectrum_identification_result_items.id AS 'SII_ID',\n" +
                    "  peptides.id AS 'PEPTIDE_ID',\n" +
                    "  peptides.sequence AS 'SEQUENCE',\n" +
                    "  peptides.encoded_sequence AS 'ENCODED_SEQUENCE', \n" +
                    "  COUNT(spectrum_identification_result_items.id) AS 'SPECTRA_COUNT'\n" +
                    "  FROM\n" +
                    "    spectrum_identification_result_items,\n" +
                    "    peptides\n" +
                    "  WHERE\n" +
                    "    peptides.id = spectrum_identification_result_items.peptide_ref\n" +
                    "GROUP BY\n" +
                    "  peptides.id";

            Statement st = MetaTableManager.dbMgr.conn.createStatement();
            int c = st.executeUpdate(q);
            st.close();
            logger.info("Created table spectrum_counts with return value %d", c);

            logger.info("Creating meta table protein_counts");
            String q2 = "CREATE TABLE protein_counts AS SELECT spectrum_identification_result_items.id AS 'SII_ID', COUNT(protein_description.accession) AS 'PROTEIN_COUNT' FROM protein_description, protein_detection_hypothesis,peptide_hypothesis, sii_peptide_evidence, spectrum_identification_result_items WHERE protein_detection_hypothesis.dbSequence_ref = protein_description.id AND protein_detection_hypothesis.id = peptide_hypothesis.protein_detection_hyp_ref AND peptide_hypothesis.peptideEvidence_ref = sii_peptide_evidence.peptide_evidence_ref AND sii_peptide_evidence.sii_id = spectrum_identification_result_items.id GROUP BY spectrum_identification_result_items.id";
            Statement st2 = MetaTableManager.dbMgr.conn.createStatement();
            st2.executeUpdate(q2);
            logger.info("Created table protein_counts.");

            logger.info("Creating INDEX proteinCountBySII_ID");
            st2.executeUpdate("CREATE INDEX proteinCountBySII_ID ON protein_counts(SII_ID)");
            logger.info( "Creating INDEX spectrumCountBySII_ID");
            st2.executeUpdate("CREATE INDEX spectrumCountBySII_ID ON spectrum_counts(SII_ID)");
            st2.executeUpdate("CREATE INDEX seqIDX ON spectrum_counts(SEQUENCE)");
            logger.info("CREATE INDEX seqIDX ON spectrum_counts(SEQUENCE)");
            st2.close();
            MetaTableManager.dbMgr.conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

}
