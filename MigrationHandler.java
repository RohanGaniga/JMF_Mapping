package com.infa.pc.migration.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.informatica.cci.reginfo.Attribute;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.ASQTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.DSQTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.LookupTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.PortDef;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTarget;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationDataTypes;
import com.informatica.powercenter.sdk.mapfwk.exception.InvalidTransformationException;
import com.informatica.powercenter.sdk.mapfwk.metaextension.MetaExtension;
import com.informatica.powercenter.sdk.mapfwk.plugin.PowerConnectConInfo;
import com.informatica.powercenter.sdk.mapfwk.plugin.PowerConnectSource;
import com.informatica.powercenter.sdk.mapfwk.plugin.PowerConnectSourceFactory;
import com.informatica.powercenter.sdk.mapfwk.plugin.PowerConnectTarget;
import com.informatica.powercenter.sdk.mapfwk.plugin.PowerConnectTargetFactory;
import com.informatica.powercenter.sdk.mapfwk.powercentercompatibility.PowerCenterCompatibility;
import com.informatica.powercenter.sdk.mapfwk.powercentercompatibility.PowerCenterCompatibilityFactory;

public abstract class MigrationHandler {

	private Properties runtimeProps;
	public void updateMappings(List<Mapping> maps, String sourceType, String targetType) throws Exception {
		for(Mapping map : maps){
			Map<String, Source> newSources = updateGetNewSource(map, sourceType, targetType);
			Map<String, Target> newTargets =  updateGetNewTarget(map,  sourceType, targetType);
			Map<String, Transformation> trans = new HashMap<>();
			trans.putAll(convertASQ(map, newSources, sourceType));
			trans.putAll(convertLookup(map, sourceType, targetType));
			updatePortDefs(map, newSources, newTargets, trans);
		}

	}
	private Map<String, Source> updateGetNewSource(Mapping map, String sourceType, String targetType) throws Exception {
		List<Source> sources = new ArrayList<>();
		Map<String, Source> newSources = new HashMap<>();
		for(Source source : map.getSources()){
			if(sourceType.equals(source.getConnInfo().getConnType().name()) || sourceType.equals(source.getConnInfo().getConnectionName())){
				PowerConnectSource s = convertSource(source, sourceType, targetType);
				sources.add(s);
				newSources.put(s.getName(), s);
			} else {
				sources.add(source);
			}
		}
		map.getSources().clear();
		map.getSources().addAll(sources);
		return newSources;
	}
	private Map<String, Target> updateGetNewTarget(Mapping map, String sourceType, String targetType) throws Exception {
		List<Target> targets = new ArrayList<>();
		Map<String, Target> newTargets = new HashMap<>();
		for(Target target : map.getTargets()){
			if(target.getConnInfo().getConnType().name().equals(sourceType) || target.getConnInfo().getConnectionName().equals(sourceType)){
				PowerConnectTarget tg = convertTarget(target, sourceType, targetType);
				targets.add(tg);
				newTargets.put(tg.getName(), tg);
			} else {
				targets.add(target);
			}
		}
		map.getTargets().clear();
		map.getTargets().addAll(targets);
		return newTargets;
	}
	private Map<String, Transformation> convertLookup(Mapping map, String sourceType, String targetType) throws Exception {
		Map<String, Transformation> newAsq = new HashMap<>();
		for(int i = 0; i < map.getTransformations().size(); i++){
			Transformation trans = map.getTransformations().get(i);
			if(trans instanceof LookupTransformation){
				Source s = trans.getTransContext().getLookupSource();
				PowerConnectSource newSource = convertSource(s, sourceType, targetType);

				Transformation asq = newSource.createASQTransform();
				updateASQ(newSource, asq, null);
				updateLookup(trans, asq.getName());
				trans.getTransContext().setLookupSrcTgt(newSource);
				map.getSources().add(newSource);
				map.getTransformations().add(asq);
				newAsq.put(asq.getName(), asq);
			}
		}
		return newAsq;
	}
	private void updateLookup(Transformation trans, String name){
		trans.getTransformationProperties().setProperty(TransformPropsConstants.LKP_TABLE_NAME, name);
		trans.getTransformationProperties().setProperty(TransformPropsConstants.CONNECTION_INFO, "");
		trans.getTransformationProperties().setProperty(TransformPropsConstants.LKP_CASE_SENSITIVE_STRING_COMPARISON, "YES");
		trans.getTransformationProperties().setProperty(TransformPropsConstants.SOURCE_TYPE, "Source Qualifier");
		for(PortDef port : trans.getTransContext().getInputSets().get(0).getPortDefs()){
			port.setFromInstanceType(TransformationConstants.ASQ);
		}
	}
	private Map<String, Transformation> convertASQ(Mapping map, Map<String, Source> newSources, String sourceType) throws InvalidTransformationException {
		Map<String, Transformation> newAsq = new HashMap<>();
		for(int i = 0; i < map.getTransformations().size(); i++){
			Transformation trans = map.getTransformations().get(i);
			if(trans instanceof DSQTransformation){
				Source s = trans.getTransContext().getInputSets().get(0).getSource();
				Source source = newSources.get(s.getName());
				if(source != null){
					Transformation asq = source.createASQTransform();
					updateASQ(source, asq, trans);
					map.getTransformations().set(i, asq);
					newAsq.put(asq.getName(), asq);
				}
			}
		}
		return newAsq;
	}
	private void updateASQ(Source source, Transformation asq, Transformation oldSQ){
		asq.getTransformationProperties().setProperty(TransformPropsConstants.TRACING_LEVEL, "Normal");
		asq.getTransformationProperties().setProperty(TransformPropsConstants.OUTPUT_IS_DETERMINISTIC, "No");

		for(Field f : asq.getTransContext().getInputSets().get(0).getOutRowSet().getFields()){
			if (f.getDataType().equals(TransformationDataTypes.DATE_TIME))
			{
				PowerCenterCompatibilityFactory compFactory = PowerCenterCompatibilityFactory.getInstance();
				PowerCenterCompatibility compInstance = compFactory.getPowerCenterCompatibilityInstance();
				f.setPrecision(compInstance.getTransformationPrecision());
				f.setScale(compInstance.getTransformationScale());
			}
		}

		for(MetaExtension ext : source.getMetaExtensions()){
			if(ext.getExtensionName().equals("ReadOperation")){
				MetaExtension me = (MetaExtension) ext.clone();
				asq.addMetaExtension(me);
				break;
			}
		}
		if(oldSQ != null){
			asq.setName(oldSQ.getName());
			asq.setInstanceName(oldSQ.getInstanceName());
			List<PortDef> portDefs = asq.getTransContext().getInputSets().get(0).getPortDefs();
	        for(PortDef ports : portDefs){
	        	ports.setToInstanceName(oldSQ.getName());
	        }
		}
	}
	private void updatePortDefs(Mapping map, Map<String, Source> newSources, Map<String, Target> newTargets,
			Map<String, Transformation> trans) {
		for(Target t : map.getTargets()){
			for(PortDef port : t.getPortDefs()){
				String fromName = port.getFromInstanceName();
				if(trans.get(fromName) != null){
					if(trans.get(fromName) instanceof ASQTransformation){
						port.setFromInstanceType(TransformationConstants.ASQ);
					}
				}
			}
		}
	}

	private PowerConnectSource convertSource(Source oldSource, String sourceType, String targetType) throws Exception {
		PowerConnectSource newSource = PowerConnectSourceFactory.getInstance().getPowerConnectSourceInstance(targetType, oldSource.getName(), oldSource.getBusinessName(), oldSource.getDescription(), oldSource.getInstanceName());
		newSource.getConnInfo().getConnProps().setProperty(ConnectionPropsConstants.DBNAME, targetType);
		String connName = System.getProperty(ConnectionPropsConstants.CONNECTIONNAME);
		newSource.getConnInfo().getConnProps().setProperty(ConnectionPropsConstants.CONNECTIONNAME, connName);
		this.addFields(newSource, oldSource);
		this.addSourceMetadataExtension(newSource);
		return newSource;
	}
	private void addFields(SourceTarget newSourceTarget, SourceTarget oldSourceTarget) {
		for(Field f : oldSourceTarget.getFields()){
			Field field = new Field(f.getName(), f.getBusinessName(), f.getDescription(), f.getDataType(), f.getPrecision(), f.getScale(), f.getFieldKeyType(), f.getFieldType(), f.isNotNull()); 
			this.updateTypes(field);
			this.addAttributeValues(field);
			newSourceTarget.addField(field);
			System.out.println(f.getDataType());
		}
	}
	private void addAttributeValues(Field field){
		for(Attribute atr : CCIHelper.getFieldAttribute()){
			field.setAttributeValues(atr.getName(), atr.getDefaultValue().toString());
		}
		updateFieldAttributeValues(field);
	}
	private void addSourceMetadataExtension(PowerConnectSource newSource) throws Exception {
		Properties pcRuntimeProps = ((PowerConnectConInfo)newSource.getConnInfo()).getCustSessionExtAttr();
		CCIHelper.populateRuntimeProps(this.runtimeProps, pcRuntimeProps);
		
		MetaExtension catalogME = new MetaExtension("SourceRecord", "STRING", "60000", CCIHelper.getCatalogSerializedObject(newSource).toString(), false);
		catalogME.setDomainName(getDomainName());
		newSource.addMetaExtension(catalogME);

		MetaExtension asoME = new MetaExtension("ReadOperation", "STRING", "900000", CCIHelper.getASOSerializedObject().toString(), false);
		asoME.setDomainName(getDomainName());
		newSource.addMetaExtension(asoME);
	}

	private PowerConnectTarget convertTarget(Target oldTarget, String sourceType, String targetType) throws Exception {
		PowerConnectTarget newTarget = PowerConnectTargetFactory.getInstance().getPowerConnectTargetInstance(targetType, oldTarget.getName(), oldTarget.getBusinessName(), oldTarget.getDescription(), oldTarget.getInstanceName());
		newTarget.getConnInfo().getConnProps().setProperty(ConnectionPropsConstants.DBNAME, targetType);
		String connName = System.getProperty(ConnectionPropsConstants.CONNECTIONNAME);
		newTarget.getConnInfo().getConnProps().setProperty(ConnectionPropsConstants.CONNECTIONNAME, connName);
		this.addFields(newTarget,oldTarget);
		this.addTargetMetadataExtension(newTarget);
		this.addPortDefs(newTarget, oldTarget);
		return newTarget;
	}
	private void addPortDefs(PowerConnectTarget newTarget, Target oldTarget) {
		newTarget.getPortDefs().clear();
		newTarget.getPortDefs().addAll(oldTarget.getPortDefs());

	}
	/*private void addFields() {
		for(Field f : oldTarget.getFields()){
			Field field = new Field(f.getName(), f.getBusinessName(), f.getDescription(), f.getDataType(), f.getPrecision(), f.getScale(), f.getFieldKeyType(), f.getFieldType(), f.isNotNull()); 
			targetHelper.setType(field);
			targetHelper.addAttributeValues(field);
			newTarget.addField(field);
			System.out.println(f.getDataType());
		}
	}*/
	private void addTargetMetadataExtension(PowerConnectTarget newTarget) throws Exception {
		Properties pcRuntimeProps = ((PowerConnectConInfo)newTarget.getConnInfo()).getCustSessionExtAttr();
		CCIHelper.populateRuntimeProperties(this.runtimeProps, pcRuntimeProps);
		
		MetaExtension catalogME = new MetaExtension("PrimaryRecord", "String", "20000", CCIHelper.getCatalogSerializedObject(newTarget).toString(), false);
		catalogME.setDomainName(getDomainName());
		newTarget.addMetaExtension(catalogME);
		
		MetaExtension asoME = new MetaExtension("WriteOperation", "String", "20000", CCIHelper.getASOSerializedObject().toString(), false);
		asoME.setDomainName(getDomainName());
		newTarget.addMetaExtension(asoME);
	}
	public void setRuntimeProps(Properties runtimeProperties) {
		this.runtimeProps = runtimeProperties;
	}
	
	protected abstract String getDomainName();
	protected abstract void updateTypes(Field field);
	protected abstract void updateFieldAttributeValues(Field field);
	protected abstract String getAdapterID();
	protected abstract HashMap<String, Object> getConnectionInfo();
}
