package edu.umn.msi.gx.fasta;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class FastaParse {

    private List<String> targetSequences;
    private List<String> regexPatterns = Collections.singletonList("^>\\w+\\|(.*?)\\|");

    Logger logger = LogManager.getLogger(this.getClass().getName());

    public FastaParse(List<String> ids) {
        this.targetSequences = ids;
    }

    /**
     * User can override the standard REGEX used for pulling out protein ID.
     *
     * @param userRX
     */
    public void setIDRegEx(List<String> userRX) {
        logger.info("Resetting my REGEX List to {}", userRX);
        regexPatterns = userRX;
    }


    public static void main(String[] args) {
        //A test.
        List<String> targetIDs = new ArrayList<>();
        targetIDs.add("P97288");
        FastaParse fp = new FastaParse(targetIDs);
        Map<String, String> r = fp.parseFASTA("/Users/mcgo0092/Documents/JavaCode/MZIdentXMLParser/data/small.fasta");
    }

    public Map<String, String> parseFASTA(String fileName) {
        Map<String, String> parsedSequences = new HashMap<>();
        logger.info("Starting FASTA parsing.");

        List<Pattern> compiledRX = new ArrayList<>();
        for (String s : regexPatterns) {
            compiledRX.add(Pattern.compile(s));
        }

        int proteinCount = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String currentProtein = null;
            StringBuilder sb = new StringBuilder();

            String line;
            while ((line = br.readLine()) != null) {

                if (line.startsWith(">")) {
                    proteinCount++;
                }

                for (Pattern p : compiledRX) {
                    Matcher matcher = p.matcher(line);
                    if (matcher.lookingAt()) {
                        if (currentProtein != null) {
                            parsedSequences.put(currentProtein, sb.toString());
                            sb = new StringBuilder();
                        }
                        currentProtein = matcher.group(1);
                    } else {
                        sb.append(line);
                    }
                }
            }
            //Grab last protein sequence
            parsedSequences.put(currentProtein, sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Created Map of {} protein sequences", parsedSequences.size());
        logger.info("Saw {} header lines during parsing.", proteinCount);

        logger.info("Filtering parsed protein sequences.");

        Map<String, String> filteredSequences = targetSequences.stream()
                .filter(parsedSequences::containsKey)
                .collect(Collectors.toMap(p -> p, parsedSequences::get));


        logger.info("Filtered to {} sequences.", filteredSequences.size());

        return filteredSequences;
    }

}
