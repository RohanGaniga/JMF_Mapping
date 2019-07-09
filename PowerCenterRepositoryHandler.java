package com.infa.pc.migration.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Properties;

import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.INameFilter;
import com.informatica.powercenter.sdk.mapfwk.core.MapFwkOutputContext;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Table;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.exception.MapFwkReaderException;
import com.informatica.powercenter.sdk.mapfwk.exception.RepoOperationException;
import com.informatica.powercenter.sdk.mapfwk.repository.PmrepRepositoryConnectionManager;
import com.informatica.powercenter.sdk.mapfwk.repository.RepoPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.repository.Repository;
import com.informatica.powercenter.sdk.mapfwk.repository.RepositoryConnectionManager;

public class PowerCenterRepositoryHandler {

	private Repository repo = new Repository( "PowerCenter", "PowerCenter", "This repository contains API test samples" );
	private Folder folder;
	private static PowerCenterRepositoryHandler instance = new PowerCenterRepositoryHandler();

	private PowerCenterRepositoryHandler() {

	}

	public static PowerCenterRepositoryHandler getInstance() {
		return instance;
	}

	public Repository connectPCRepo(Properties properties) throws Exception {

		// Init the repository connection properties
		repo.getRepoConnectionInfo().setPcClientInstallPath(properties.getProperty(RepoPropsConstants.PC_CLIENT_INSTALL_PATH) + "\\PowerCenterClient\\client\\bin");
		repo.getRepoConnectionInfo().setTargetFolderName(properties.getProperty(RepoPropsConstants.TARGET_FOLDER_NAME));
		repo.getRepoConnectionInfo().setTargetRepoName(properties.getProperty(RepoPropsConstants.TARGET_REPO_NAME));
		repo.getRepoConnectionInfo().setRepoServerHost(properties.getProperty(RepoPropsConstants.REPO_SERVER_HOST));
		repo.getRepoConnectionInfo().setAdminPassword(properties.getProperty(RepoPropsConstants.ADMIN_PASSWORD));
		repo.getRepoConnectionInfo().setAdminUsername(properties.getProperty(RepoPropsConstants.ADMIN_USERNAME));
		repo.getRepoConnectionInfo().setRepoServerPort(properties.getProperty(RepoPropsConstants.REPO_SERVER_PORT));
		repo.getRepoConnectionInfo().setServerPort(properties.getProperty(RepoPropsConstants.SERVER_PORT));
		repo.getRepoConnectionInfo().setDatabaseType(properties.getProperty(RepoPropsConstants.DATABASETYPE));

		if(properties.getProperty(RepoPropsConstants.PMREP_CACHE_FOLDER) != null)
			repo.getRepoConnectionInfo().setPmrepCacheFolder(properties.getProperty(RepoPropsConstants.PMREP_CACHE_FOLDER));

		RepositoryConnectionManager repmgr = new PmrepRepositoryConnectionManager();
		repo.setRepositoryConnectionManager(repmgr);

		repmgr.connect();

		return repo;
	}

	private List<Mapping> getMappings(Folder folder) throws RepoOperationException, MapFwkReaderException {
		if(folder.getMappings().isEmpty()){
			folder.fetchMappingsFromRepository();
		}
		return folder.getMappings();
	}

	public List<Mapping> getMappings(String folderName) throws Exception {
		this.folder = getFolder(folderName);
		return getMappings(folder);
	}

	private Folder getFolder(String folderName) throws RepoOperationException, MapFwkReaderException, FileNotFoundException {
		if(this.folder != null && this.folder.getName().equals(folderName)){
			return folder;
		}
		List<Folder> fList = repo.getFolders(new INameFilter() {
			@Override
			public boolean accept(String name) {
				return name.equals(folderName);
			}
		});

		if(fList.isEmpty()){
			throw new FileNotFoundException(folderName + " does not exists");
		}
		return fList.get(0);
	}

	private MigrationHandler getMappingUtility(String targetType) {
		return MigrationHandlerFactory.getMigrationHandler(targetType);
	}

	public void updateMappings(List<Mapping> maps, String sourceType, String targetType) throws Exception {
		getMappingUtility(targetType).updateMappings(maps, sourceType, targetType);
	}

	public void saveMappings(List<Mapping> mapings) throws Exception {
		createTargetFolder();

		repo.clear();
		repo.addFolder(mapings.get(0).getParentFolder());
		for(Mapping maping : mapings){
			maping.setModified(true);
			updateWorkflows(maping.getParentFolder(), maping);
		}
		boolean doImport = true;
		String targetDirName = repo.getRepoConnectionInfo().getTargetFolderName();
		MapFwkOutputContext outputContext = new MapFwkOutputContext(MapFwkOutputContext.OUTPUT_FORMAT_XML,
				MapFwkOutputContext.OUTPUT_TARGET_FILE, System.getProperty("user.dir")+File.separator + targetDirName + ".xml");
		repo.save(outputContext, doImport, true);
		System.out.println("Mapping generated in " + mapings);
	}

	private void createTargetFolder() throws RepoOperationException, MapFwkReaderException {
		repo.clear();
		List<Folder> fList = repo.getFolders(new INameFilter() {

			@Override
			public boolean accept(String name) {
				return name.equals(repo.getRepoConnectionInfo().getTargetFolderName());
			}
		});
		if(fList == null || fList.isEmpty()){
			Folder folder=new Folder(repo.getRepoConnectionInfo().getTargetFolderName(),repo.getRepoConnectionInfo().getTargetFolderName(),"New migration folder");
			repo.createFolder(folder, false);
		}
	}

	private void updateWorkflows(Folder folder, Mapping maping) throws RepoOperationException, MapFwkReaderException {
		List<Workflow> workflows = null ;
		if(folder.getWorkFlows().isEmpty()){
			workflows = folder.fetchWorkflowsFromRepository();
		}else {
			workflows = folder.getWorkFlows();
		}
		for(Workflow wf : workflows){
			boolean modified = false;
			if(!wf.getSessions().isEmpty()){
				for(Session s : wf.getSessions()){
					if(s.getMapping().getName().equals(maping.getName())){
						s.setMapping(maping);
						setConnectionInfos(s.getObjectInstances(), maping);
						modified = true;
					}
				}
				if(modified){
					wf.setModified(true);
					maping.getParentFolder().addWorkFlow(wf);
				}
			}
		}
	}

	private void setConnectionInfos(List<Table> objectInstances, Mapping maping) {
		String newConnName = System.getProperty(ConnectionPropsConstants.CONNECTIONNAME);
		if(newConnName == null){
			newConnName = "";
		}
		for(Table obj : objectInstances){
			if(obj instanceof Source){
				List<Source> sources = maping.getSources();
				for(Source s : sources){
					if(s.getName().equals(((Source)obj).getName())){
						String str = (String) ((Source)obj).getConnInfo().getConnProps().get(ConnectionPropsConstants.CONNECTIONNAME);
						s.getConnInfo().getConnProps().setProperty(ConnectionPropsConstants.CONNECTIONNAME, newConnName);
					}
				}
			}else if(obj instanceof Target){

				List<Target> targets = maping.getTargets();
				for(Target t : targets){
					if(t.getName().equals(((Target)obj).getName())){
						String str = (String) ((Target)obj).getConnInfo().getConnProps().get(ConnectionPropsConstants.CONNECTIONNAME);
						t.getConnInfo().getConnProps().setProperty(ConnectionPropsConstants.CONNECTIONNAME, newConnName);
					}
				}

			}
		}

	}
}
