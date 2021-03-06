package edu.umn.msi.gx.java;

import edu.umn.msi.gx.database.DatabaseManager;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.ext.DefaultHandler2;
import org.xml.sax.helpers.XMLReaderFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileReader;

public class ThreadedXMLReader implements Runnable {

    XMLReader mzidReader;
    String xmlFileName;
    DefaultHandler2 contentHandler;
    private DatabaseManager dbMgr;

    private Logger logger;

    public ThreadedXMLReader(String fileName, DefaultHandler2 handler) {
        this.xmlFileName = fileName;
        this.contentHandler = handler;
        logger = LogManager.getFormatterLogger("ThreadedXMLReader->" + this.contentHandler.getClass().getName());
        try {
            this.mzidReader = XMLReaderFactory.createXMLReader();
            this.mzidReader.setContentHandler(this.contentHandler);
        } catch (SAXException se) {}

    }

    public void setDbMgr(DatabaseManager d) {
        this.dbMgr = d;
    }

    //Do this for each content handler.
    public void generateTablesAndData() {
        SaxContentHandler sh = (SaxContentHandler)this.contentHandler;
        sh.generateSQL(this.dbMgr);
    }

    @Override
    public void run() {
        try {
            logger.info("Parsing file %s", this.xmlFileName);
            this.mzidReader.parse(new InputSource(new FileReader(this.xmlFileName)));
        } catch (Exception e) {
        }
    }
}
