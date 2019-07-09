package com.infa.pc.migration.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.repository.RepoPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.util.Logger;

public class SampleMappingMigrator {

	private static String filename = "resources\\pcconfig.properties";
	private static String SOURCE_CONNECTION_TYPE;
	private static String TARGET_CONNECTION_TYPE;
	private static Properties properties;
	private static String SOURCE_FOLDER_NAME;
	private static final Logger LOGGER = Logger.getLogger(SampleMappingMigrator.class.getName());

	static {

		try {
			//Load configuration file
			InputStream propStream = new FileInputStream(filename);
			properties = new Properties();
			properties.load(propStream);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.setProperty("javax.xml.transform.TransformerFactory", "com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl");
		System.setProperty("CLIENT_INSTALL_DIR", properties.getProperty(RepoPropsConstants.PC_CLIENT_INSTALL_PATH));
		System.setProperty("CONNECTIONNAME", properties.getProperty(ConnectionPropsConstants.CONNECTIONNAME));
		System.setProperty("app.home", System.getProperty("CLIENT_INSTALL_DIR") + "\\PowerCenterClient\\MappingSDK\\");
		System.setProperty("CCI_HOME", System.getProperty("CLIENT_INSTALL_DIR") + "\\PowerCenterClient\\cci");
		System.setProperty("ADAPTER_HOME", System.getProperty("CLIENT_INSTALL_DIR") + "\\PowerCenterClient\\connectors");
		System.setProperty("INFA_OSGI_CACHE_DIR", System.getProperty("CLIENT_INSTALL_DIR") + "\\PowerCenterClient\\client\\bin\\workspace\\osgiCache");
		System.setProperty("INFA_THIRDPARTY_LIB", System.getProperty("CLIENT_INSTALL_DIR") + "\\PowerCenterClient");
		System.setProperty("INFA_FRAMEWORK_STORAGE_CLEAN", "RETAIN");
		SOURCE_FOLDER_NAME = properties.getProperty("SOURCE_FOLDER_NAME");
		SOURCE_CONNECTION_TYPE = properties.getProperty("SOURCE_CONNECTION_TYPE");
		TARGET_CONNECTION_TYPE = properties.getProperty("TARGET_CONNECTION_TYPE");

		// Assigning handlers to LOGGER object
		LOGGER.configure("resources\\logging.properties");

	}
	
	//Load the SnowFlakeDataWarehouse_runtime properties file
	private static Properties loadRuntimeProperties() throws IOException {
		Properties runtimeProps = new Properties();
		String filename = "resources\\"+ "SnowflakeCloudDataWarehouse_runtime.properties";
		File propFile = new File(filename);
		if(!propFile.exists()){
			System.out.println("Runtime properties file does not exists, creating the file");
			propFile.createNewFile();
		}
		InputStream propStream = new FileInputStream(propFile);//getClass().getClassLoader().getResourceAsStream( filename);
		runtimeProps.load( propStream );
		return runtimeProps;
	}

	public static void main(String[] args) {

		try {
			/*Setup configuration*/
			setupMigrationConfiguration();

			/*Start Migration*/
			migrate();

		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Exception is: " + e.getMessage());
		}
	}

	/**
	 * Initialize CCI and create repository object
	 * @throws Exception
	 */
	private static void setupMigrationConfiguration() throws Exception {
		/* Initialize CCI */
		initCCI();
		/* Connect to PowerCenter repository */
		connectPCRepo(properties);
	}

	private static void initCCI() {
		CCIHelper.init(TARGET_CONNECTION_TYPE);
	}
	
	private static void connectPCRepo(Properties properties) throws Exception {
		PowerCenterRepositoryHandler.getInstance().connectPCRepo(properties);
		MigrationHandlerFactory.getMigrationHandler(TARGET_CONNECTION_TYPE).setRuntimeProps(loadRuntimeProperties());
	}
	
	private static void migrate() throws Exception {
		PowerCenterRepositoryHandler pcRepoInstance = PowerCenterRepositoryHandler.getInstance();
		
		/* Fetch all mappings from the specified folder */
		List<Mapping> mappings = pcRepoInstance.getMappings(SOURCE_FOLDER_NAME);
		
		/*Convert mapping object having SOURCE_CONNECTION_TYPE in source or target to respective TARGET_CONNECTION_TYPE as source and target.*/
		pcRepoInstance.updateMappings(mappings, SOURCE_CONNECTION_TYPE, TARGET_CONNECTION_TYPE);
		
		/*Save the updated mapping to Repository, including all the impacted workflows.*/
		pcRepoInstance.saveMappings(mappings);
	}
}
