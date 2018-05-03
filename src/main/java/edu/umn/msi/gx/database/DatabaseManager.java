package edu.umn.msi.gx.database;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sqlite.SQLiteConfig;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Ensures a single connection to the SQLite database.
 * Sets the configuration for the database.
 * Journal mode has been set to <b>OFF</b> to increase speed. There
 * is not rollback behavior available;
 */
public class DatabaseManager {

    private Logger logger = LogManager.getLogger(DatabaseManager.class.getName());

    private String url;
    public Connection conn;

    public DatabaseManager(String fileName){
        this.url = "jdbc:sqlite:" + fileName;
    }

    public void createNewDatabase(){
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            SQLiteConfig sqLiteConfig = new SQLiteConfig();
            sqLiteConfig.setPragma(SQLiteConfig.Pragma.JOURNAL_MODE, "OFF");
            conn = DriverManager.getConnection(this.url, sqLiteConfig.toProperties());
            conn.setAutoCommit(false);
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                logger.info("The driver name is " + meta.getDriverName());
            }

        } catch (SQLException se) {
            System.out.println(se.getMessage());
        }
    }
}
