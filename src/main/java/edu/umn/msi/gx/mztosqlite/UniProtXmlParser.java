/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.umn.msi.gx.mztosqlite;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author James E Johnson jj@umn.edu
 * @version 
 */
public class UniProtXmlParser {

    HashMap<String, String> accToSeq = new HashMap<>();

    public void readUniprotXML(String uniprotxml) {
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser saxParser = factory.newSAXParser();

            DefaultHandler handler = new DefaultHandler() {
                boolean inSequence = false;
                boolean inAccession = false;
                List<String> accessions = new ArrayList<>();
                String sequence = null;

                public void startElement(String uri, String localName, String qName,
                        Attributes attributes) throws SAXException {
                    System.out.println("Start Element :" + qName);
                    if (qName.equalsIgnoreCase("entry")) {
                        accessions.clear();
                        sequence = null;
                    } else if (qName.equalsIgnoreCase("accession")) {
                        inAccession = true;
                    } else if (qName.equalsIgnoreCase("sequence")) {
                        inSequence = true;
                    }
                }

                public void endElement(String uri, String localName,
                        String qName) throws SAXException {

                    System.out.println("End Element :" + qName);
                    if (qName.equalsIgnoreCase("entry")) {
                        for (String acc : accessions) {
                            System.out.println(acc + " : " + sequence);
                            accToSeq.put(acc, sequence);
                        }
                        accessions.clear();
                        sequence = null;
                    }
                }

                public void characters(char ch[], int start, int length) throws SAXException {
                    if (inAccession) {
                        accessions.add(new String(ch, start, length));
                        inAccession = false;
                    } else if (inSequence) {
                        sequence = new String(ch, start, length).replaceAll("\\s", "");
                        inSequence = false;
                    }
                }
            };
            saxParser.parse(new File(uniprotxml), handler);

        } catch (ParserConfigurationException ex) {
            Logger.getLogger(MzIdentParser.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SAXException ex) {
            Logger.getLogger(MzIdentParser.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(MzIdentParser.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void main(String[] argv) {
        UniProtXmlParser parser = new UniProtXmlParser();
        String uniprot = argv.length > 0 ? argv[0] : "/Users/jj/gx/gh/gxp/tools-galaxyp/tools/morpheus/test-data/uniprot-proteome_UP000002311Condensed-first100entries.xml";
        parser.readUniprotXML(uniprot);
    }
}
