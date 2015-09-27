package com.project.MRCube;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

public class RegionsGenerator {
	
	private static final String CONNECTION_URL_PROPERTY = "connection.url";
	private static final String JDBC_DRIVER_NAME_PROPERTY = "jdbc.driver.class.name";

	private static String connectionUrl;
	private static String jdbcDriverName;
	private static String createStatement = "CREATE TABLE %s AS SELECT %s FROM sample_07";
	

        private static void loadConfiguration() throws IOException {
                InputStream input = null;
                try {
                        String filename = "MRCube.conf";
                        input = RegionsGenerator.class.getClassLoader().getResourceAsStream(filename);
                        Properties prop = new Properties();
                        prop.load(input);
        
                        connectionUrl = prop.getProperty(CONNECTION_URL_PROPERTY);
                        jdbcDriverName = prop.getProperty(JDBC_DRIVER_NAME_PROPERTY);
                } finally {
                        try {
                                if (input != null)
                                        input.close();
                        } catch (IOException e) {
                                // nothing to do
                        }
                }
        }

	public static void main(String[] args) throws IOException {

		//Get file from resources folder
		ClassLoader classLoader = RegionsGenerator.class.getClassLoader();
		File file = new File(classLoader.getResource("Regions.conf").getFile());

		try (Scanner scanner = new Scanner(file)) {

			while (scanner.hasNextLine()) {
				String line = scanner.nextLine();
				String[] regions = line.split(",", 2);
				
				createRegion(regions[0], regions[1].trim());
			}

			scanner.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		
	}
	
	
	public static void createRegion(String tableName, String columnName) throws IOException {
		String sqlStatement = String.format(createStatement, tableName, columnName);

		System.out.println("\n=============================================");
		System.out.println(String.format("Creating Table %s (%s)", tableName, columnName));
		System.out.println("Running Query: " + sqlStatement);

        loadConfiguration();

		Connection con = null;

		try {

			Class.forName(jdbcDriverName);

			con = DriverManager.getConnection(connectionUrl);

			Statement stmt = con.createStatement();

			ResultSet rs = stmt.executeQuery(sqlStatement);

			System.out.println("\n== Region Created ======================");

		} catch (Exception e) {
			System.out.println("Warning: Region is already present");
		} finally {
			try {
				con.close();
			} catch (Exception e) {
				// swallow
			}
		}
	}
	
	
	
}
