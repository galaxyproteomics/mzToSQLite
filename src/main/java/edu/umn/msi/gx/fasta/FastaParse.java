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
    private List<String> regexPatterns = new ArrayList<>();

    Logger logger = LogManager.getLogger(this.getClass().getName());

    public FastaParse(List<String> ids) {
        //Add FASTA header patterns
        regexPatterns.add("^>sp.*?\\|(.+?)\\|");
        regexPatterns.add("^>tr.*?\\|(.+?)\\|");
        regexPatterns.add("^>generic.*?\\|(.+?)\\|");
        regexPatterns.add("^>(NP_.+?)\\|");
        regexPatterns.add("^>(ENS.*?)[\\s|;]");
        regexPatterns.add("^>sw.*?\\|(.+?)\\|");
        regexPatterns.add("^>gi.*?\\|(.+?)\\|");
        targetSequences = ids;
    }

    /**
     * User can override the standard REGEX used for pulling out protein ID.
     *
     * @param userRX
     */
    public void setIDRegEx(List<String> userRX) {
        logger.info("Adding to my REGEX list {}", userRX);
        //regexPatterns.add();TODO:
    }


    public static void main(String[] args) {
        //A test.
        List<String> targetIDs = new ArrayList<>();
        targetIDs.add("NP_001229242_476:TCTAC>TCTACTAC");
        FastaParse fp = new FastaParse(targetIDs);
        Map<String, String> r = fp.parseFASTA("/Users/mcgo0092/Documents/JavaCode/MZIdentXMLParser/data/ten.fasta");
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
                    //what kind of header??
                    boolean needHeader = true;
                    for (Pattern p : compiledRX) {
                        Matcher matcher = p.matcher(line);
                        if (matcher.lookingAt()) {
                            needHeader = false;
                            currentProtein = matcher.group(1);
                        }
                    }
                    if (needHeader) {
                        //This is a protein header we don't know anything about
                        //It will be ignored during filtering, log here for viewing.
                        currentProtein = line;
                        logger.info("Unknown protein header type -> '" + line + "'");
                    }
                } else {
                    //line is not starting with >, we are in the sequence.
                    sb.append(line);
                }
                if (sb.length() > 0) {
                    parsedSequences.put(currentProtein, sb.toString());
                    sb = new StringBuilder();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("Created Map of {} protein sequences", parsedSequences.size());

        logger.info("Filtering parsed protein sequences.");

        Map<String, String> filteredSequences = targetSequences.stream()
                .filter(parsedSequences::containsKey)
                .collect(Collectors.toMap(p -> p, parsedSequences::get));


        logger.info("Filtered to {} sequences.", filteredSequences.size());

        return filteredSequences;
    }

}
