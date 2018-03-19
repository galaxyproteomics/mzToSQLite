package edu.umn.msi.gx.java;

import edu.umn.msi.gx.database.DatabaseManager;

public interface SaxContentHandler {
    public void generateSQL(DatabaseManager d);
}
