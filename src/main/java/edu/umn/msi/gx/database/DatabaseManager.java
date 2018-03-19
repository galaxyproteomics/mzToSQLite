package edu.umn.msi.gx.database;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

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
            conn = DriverManager.getConnection(this.url);
            conn.setAutoCommit(false);
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                logger.debug("The driver name is " + meta.getDriverName());
            }

        } catch (SQLException se) {
            System.out.println(se.getMessage());
        }
    }
}
