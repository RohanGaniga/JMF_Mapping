package com.infa.pc.migration.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.informatica.adapter.sdkadapter.logical.semantic.auto.SAL_Node;
import com.informatica.adapter.sdkadapter.patternblocks.field.P_Field;
import com.informatica.adapter.sdkadapter.patternblocks.field.semantic.manual.SP_Field;
import com.informatica.adapter.sdkadapter.utils.ExtensionManager;
import com.informatica.cci.reginfo.Adapter;
import com.informatica.cci.reginfo.AdapterInfo;
import com.informatica.cci.reginfo.Attribute;
import com.informatica.cci.reginfo.CapabilityEnum;
import com.informatica.cci.reginfo.EnvParams;
import com.informatica.cci.reginfo.NativeMetadataObjectType;
import com.informatica.cci.reginfo.OperationType;
import com.informatica.cci.reginfo.connection.ConnectionType;
import com.informatica.cloud.adapter.cci.utils.MgfUtils;
import com.informatica.imf.io.Serializer;
import com.informatica.imf.io.impl.XMLSerializerImpl;
import com.informatica.metadata.common.typesystem.DataType;
import com.informatica.powercenter.sdk.mapfwk.core.FieldKeyType;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTarget;
import com.informatica.sdk.adapter.metadata.ASOOperation;
import com.informatica.sdk.adapter.metadata.Catalog;
import com.informatica.sdk.adapter.metadata.Connection;
import com.informatica.sdk.adapter.metadata.ConnectionInfo;
import com.informatica.sdk.adapter.metadata.ConnectionInfoBuilder;
import com.informatica.sdk.adapter.metadata.MetadataAdapter;
import com.informatica.sdk.adapter.metadata.common.CCatalogImportOpts;
import com.informatica.sdk.adapter.metadata.impl.ASOOperationImpl;
import com.informatica.sdk.adapter.metadata.impl.CatalogImpl;
import com.informatica.sdk.adapter.metadata.patternblocks.container.semantic.iface.Package;
import com.informatica.sdk.adapter.metadata.patternblocks.field.semantic.iface.Field;
import com.informatica.sdk.adapter.metadata.patternblocks.field.semantic.iface.FieldTypeEnum;
import com.informatica.sdk.adapter.metadata.patternblocks.flatrecord.semantic.iface.FlatRecord;
import com.informatica.sdk.adapter.metadata.utils.AdapterUtils;
import com.informatica.sdk.metadata.common.Property;
import com.informatica.sdk.metadata.framework.MetadataBuilderFactory;

public class CCIHelper {

	private static CCIHelper instance = new CCIHelper();
	private String adpID;
	private ConnectionType connType;
	private Catalog catalog;
	private NativeMetadataObjectType nmo;
	private static Thread tInit;
	private HashMap<String,String> capAttributes;
	private String targetType;

	private CCIHelper() {
		
	}
	
	public static CCIHelper getInstance() {
		return instance;
	}
	
	public static Object getCatalogSerializedObject(SourceTarget st) throws Exception{
		return serializeObject(((CatalogImpl) instance.createCatalog(st)).getSdkObject()._get_imfObject());
	}
	
	public static Object getASOSerializedObject() throws Exception{
		return serializeObject(((ASOOperationImpl) instance.createAso()).getSdkObject()._get_imfObject());
	}
	
	public static void populateRuntimeProperties(Properties runtimeProps, Properties pcRuntimeProps){
		instance.populateRuntimeProps(runtimeProps, pcRuntimeProps);
	}
	
	public static List<Attribute> getFieldAttribute() {
		if(getInstance().nmo == null){
			while(tInit.isAlive());
			getInstance().getConnectionType();
		}
		
		return getInstance().nmo.getFieldAttributes();
	}
	
	public static void init(String targetConnType) {
		instance.targetType = targetConnType;
		instance.adpID = MigrationHandlerFactory.getMigrationHandler(targetConnType).getAdapterID();
		tInit = new Thread(){
			public void run() {
				EnvParams env = new EnvParams();
				env.addPluginLocation(System.getProperty("CCI_HOME") + "/plugins/infa"); //$NON-NLS-1$ //$NON-NLS-2$
				env.addPluginLocation(System.getProperty("CCI_HOME") + "/plugins/osgi"); //$NON-NLS-1$ //$NON-NLS-2$

				instance.addSubfolders(new File(
						System.getProperty("ADAPTER_HOME") + File.separator + "cci" + File.separator + "plugins"), env); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

				String clean=System.getProperty("INFA_FRAMEWORK_STORAGE_CLEAN");

				env.setDeleteWorkDirOnExit(true);

				if(clean!=null && "RETAIN".equalsIgnoreCase(clean)){
					env.setDeleteWorkDirOnExit(false);
				}
				env.setOsgiWorkDir(System.getProperty("INFA_OSGI_CACHE_DIR"));
				AdapterInfo.INSTANCE.initEnv(env);
				//		System.out.println(env);
			};
		};
		tInit.start();
	}
	private void addSubfolders(File folder, EnvParams env){
		env.addPluginLocation(folder.getAbsolutePath());
		for(File child : folder.listFiles()){
			if(child.isDirectory()){
				addSubfolders(child, env);
			}
		}
	}
	private ConnectionType getConnectionType(){

		if(connType == null){
			Adapter adp = AdapterInfo.INSTANCE.getAdapterById(adpID);
			List<ConnectionType> connTypes = adp.getConnectionTypes();
			this.connType = connTypes.get(0);
			this.nmo = adp.getNativeMetadataObjectTypes().get(0);
		}
		return connType;
	}
	
	private Catalog createCatalog(SourceTarget st) throws Exception {
		/*List<Option> options = new ArrayList<Option>();
		catalog = CatalogHelper.createCatalog(adpID, getConnectionType().getNMOType());
		mdAdapter.addMetadataToCatalog(mconnection, options, catalog);
		return catalog;*/
		Adapter adp = AdapterInfo.INSTANCE.getAdapterById(adpID);
		List<Attribute> attrs = adp.getNativeMetadataObjectTypes().get(0).getWriteCapAttributes();
		catalog = MgfUtils.createCatalog(adpID, getConnectionType().getNMOType());
		
		Package pack = ((CatalogImpl)catalog).getSdkObject().getFactory().newPackage(((CatalogImpl)catalog).getSdkObject());
		pack.setName("PACKAGE");
		
		FlatRecord rec = ((CatalogImpl)catalog).getSdkObject().getFactory().newFlatRecord(((CatalogImpl)catalog).getSdkObject());
		rec.setName(st.getName());
		
		List<P_Field> list = new ArrayList<>();
		ExtensionManager extMgr = ExtensionManager.getInstance();
		
		for(com.informatica.powercenter.sdk.mapfwk.core.Field f : st.getFields()){
			Field field = ((CatalogImpl)catalog).getSdkObject().getFactory().newField(((CatalogImpl)catalog).getSdkObject());
			field.setName(f.getName());
			field.setNativeName(f.getName());
			field.setFieldTypeEnum(FieldTypeEnum.INOUT_TYPE);
			DataType dType= extMgr.getDatatypeFromString(adpID, getConnectionType().getNMOType(), getDatatype(f.getDataType()));
//			DataType type = TypeSystemUtils.INSTANCE.getDataType(Platform.typesystem, getDatatype(f.getDataType()));
			field.setDataType(dType.getName());
			field.setPrecision(Integer.parseInt(f.getPrecision()));
			field.setScale(Integer.parseInt(f.getScale()));
			field.setNullableField(f.isNotNull());
			
			if(f.getFieldKeyType() == FieldKeyType.PRIMARY_KEY){
				list.add((((SP_Field)field)._get_imfObject()));
			}
			rec.addField(field);
		}
		
		/*if((PrimaryKeyImpl)rec.getPrimaryKey() == null){
			PrimaryKey pk = (PrimaryKey) SAP_PrimaryKey.newObj((SL_ContainerObj)((FlatRecordImpl)rec).getSdkObject().getCatalog());
			((FlatRecordImpl)rec).getSdkObject().addPrimaryKey(pk);
		}
		((PrimaryKeyImpl)rec.getPrimaryKey()).getSdkObject().setFields(list);*/
		
		pack.addChildRecord(rec);
		
		((CatalogImpl)catalog).getSdkObject().addNode((SAL_Node) pack);
		return catalog;
		

	}
	
	private String getDatatype(String dataType) {
		switch(dataType.toLowerCase()){
			case "integer":
			case "byteint":
			case "smallint":
			case "bigint" : return "NUMBER";
			case "date": return "DATE";
			case "time":
			case "timestamp": return "TIMESTAMP";
			case "char":
			case "byte":
			case "varbyte":
			case "varchar": return "VARCHAR";
			case "decimal":
			case "float": return "FLOAT"; 
		}
		return "OBJECT";
	}

	private ASOOperation createAso() throws Exception {
		ASOOperation operation = MgfUtils.createDefaultWriteOperation(adpID, getConnectionInfo(), getOperationTypeName(CapabilityEnum.write), catalog, capAttributes);
		return operation;
	}

	public static void populateRuntimeProps(Properties runtimeProps, Properties pcRuntimeProps) {
		List<Attribute> capAttrs = getInstance().nmo.getWriteCapAttributes();
		getInstance().capAttributes = new HashMap<>();
		for (Attribute attr : capAttrs) {
			String key = attr.getName();
			String value = (runtimeProps.getProperty(key) != null)? runtimeProps.getProperty(key):(String) attr.getDefaultValue().toString();
			getInstance().capAttributes.put(attr.getName(), value);
			if(pcRuntimeProps.get(attr.getDisplayName()) != null){
				pcRuntimeProps.put(attr.getDisplayName(), value);
			}
		}
	}
	private String getOperationTypeName(CapabilityEnum capability) {
		List<OperationType> opTypes = nmo.getOperationTypes();
		for (OperationType opType : opTypes) {
			List<com.informatica.cci.reginfo.Capability> caps = opType
					.getCapabilities();
			for (com.informatica.cci.reginfo.Capability cap : caps) {
				if (cap.getCapabilityType().compareTo(capability) == 0)
					return opType.getName();
			}
		}
		return null;
	}
	
	private HashMap<String, Object> getConnectionInfo() {
		return MigrationHandlerFactory.getMigrationHandler(targetType).getConnectionInfo();
	}

	private static Object serializeObject(Object obj) throws Exception {
		OutputStream bostream = new ByteArrayOutputStream();
		Serializer serializer = null;
		serializer = new XMLSerializerImpl(bostream);
		serializer.serialize(Arrays.asList(new Object[] { obj }));

		return bostream;
	}
}
