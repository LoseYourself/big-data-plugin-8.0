/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2017 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.hive.trans;

import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.metastore.MetaStoreConst;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.steps.textfileoutput.TextFileOutputMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Node;

@Step( id = "HiveOutput", image = "HDO.svg", name = "HiveOutput.Name",
    description = "HiveOutput.Description",
    documentationUrl = "http://wiki.pentaho.com/display/EAI/Hadoop+File+Output",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.BigData",
    i18nPackageName = "org.pentaho.di.trans.steps.hadoopfileoutput" )
@InjectionSupported( localizationPrefix = "HiveOutput.Injection.", groups = { "OUTPUT_FIELDS" } )
public class HiveOutputMeta extends TextFileOutputMeta {

  // for message resolution
  private static Class<?> PKG = HiveOutputMeta.class;

  private String sourceConfigurationName;
  
  private String connection;
  private String hiveTableName;
  private String hiveTableId;

  private static final String SOURCE_CONFIGURATION_NAME = "source_configuration_name";

  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;
  private IMetaStore metaStore;
  private Node embeddedNamedClusterNode;

  public HiveOutputMeta( NamedClusterService namedClusterService,
                               RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester ) {
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
  }

  @Override
  public void setDefault() {
    // call the base classes method
    super.setDefault();

    // now set the default for the
    // filename to an empty string
    setFileName( "" );

    // setFileAppended(false);
    // setSeparator(";");
    // setEnclosure("\"");
    // setHeaderEnabled(false);
    // setFooterEnabled(false);
    // setFileFormat("Unix");
    // setFileCompression("None");
    // setEnclosure("UTF-8");
    // setExtension("txt");
    // setDateInFilename(true);
    // setTimeInFilename(true);

    setConnection( "" );
    setHiveTableName( "" );
    setHiveTableId( "" );

    super.setFileAsCommand( false );
  }

  @Override
  public void setFileAsCommand( boolean fileAsCommand ) {
    // Don't do anything. We want to keep this property as false
    // Throwing a KettleStepException would be desirable but then we
    // need to change the base class' method which is
    // open source.

    throw new RuntimeException( new RuntimeException( BaseMessages.getString( PKG,
        "HiveOutput.MethodNotSupportedException.Message" ) ) );
  }

  public String getSourceConfigurationName() {
    return sourceConfigurationName;
  }

  public void setSourceConfigurationName( String ncName ) {
    this.sourceConfigurationName = ncName;
  }

  public String getConnection() {
    return connection;
  }

  public void setConnection( String connection ) {
    this.connection = connection;
  }

  public String getHiveTableName() {
    return hiveTableName;
  }

  public void setHiveTableName( String hiveTableName ) {
    this.hiveTableName = hiveTableName;
  }

  public String getHiveTableId() {
    return hiveTableId;
  }

  public void setHiveTableId( String hiveTableId ) {
    this.hiveTableId = hiveTableId;
  }

  protected String loadSource( Node stepnode, IMetaStore metastore ) {
    this.metaStore = metastore;
    String url = XMLHandler.getTagValue( stepnode, "file", "name" );
    sourceConfigurationName = XMLHandler.getTagValue( stepnode, "file", SOURCE_CONFIGURATION_NAME );
    embeddedNamedClusterNode = XMLHandler.getSubNode( stepnode, "NamedCluster" );

    hiveTableName = XMLHandler.getTagValue( stepnode, "table" );
    hiveTableId = XMLHandler.getTagValue( stepnode, "tableId" );

    return getProcessedUrl( metastore, url );
  }

  protected String getProcessedUrl( IMetaStore metastore, String url ) {
    if ( url == null ) {
      return null;
    }
    if ( metastore == null ) {
      // Maybe we can get a metastore from spoon
      try {
        metaStore = MetaStoreConst.openLocalPentahoMetaStore( false );
      } catch ( Exception e ) {
        // If no local metastore we must ignore and proceed
      }
    } else {
      // if we already have a metastore use it
      metaStore = metastore;
    }
    NamedCluster c = null;
    if ( metaStore != null ) {
      // If we have a metastore get the cluster from it.
      c = namedClusterService.getNamedClusterByName( sourceConfigurationName, metaStore );
    } else {
      // Still no metastore, try to make a named cluster from the embedded xml
      if ( namedClusterService.getClusterTemplate() != null ) {
        c = namedClusterService.getClusterTemplate().fromXmlForEmbed( embeddedNamedClusterNode );
      }
    }
    if ( c != null ) {
      url = c.processURLsubstitution( url, metaStore, new Variables() );
    }
    return url;
  }

  protected void saveSource( StringBuilder retVal, String fileName ) {
    retVal.append( "      " ).append( XMLHandler.addTagValue( "name", fileName ) );
    retVal.append( "      " ).append( XMLHandler.addTagValue( SOURCE_CONFIGURATION_NAME, sourceConfigurationName ) );

    retVal.append( "      " ).append( XMLHandler.addTagValue( "table", hiveTableName ) );
    retVal.append( "      " ).append( XMLHandler.addTagValue( "tableId", hiveTableId ) );
  }

  @Override
  public String getXML() {
    String xml = super.getXML();
    NamedCluster c = namedClusterService.getNamedClusterByName( sourceConfigurationName, metaStore );
    if ( c != null ) {
      xml = xml + c.toXmlForEmbed( "NamedCluster" )  + Const.CR;
    }
    return xml;
  }

  // Receiving metaStore because RepositoryProxy.getMetaStore() returns a hard-coded null
  protected String loadSourceRep( Repository rep, ObjectId id_step,  IMetaStore metaStore ) throws KettleException {
    this.metaStore = metaStore;
    String url = rep.getStepAttributeString( id_step, "file_name" );
    sourceConfigurationName = rep.getStepAttributeString( id_step, SOURCE_CONFIGURATION_NAME );

    hiveTableName = rep.getStepAttributeString( id_step, "table" );
    hiveTableId = rep.getStepAttributeString( id_step, "tableId" );

    return getProcessedUrl( metaStore, url );
  }

  protected void saveSourceRep( Repository rep, ObjectId id_transformation, ObjectId id_step, String fileName )
    throws KettleException {
    rep.saveStepAttribute( id_transformation, id_step, "file_name", fileName );
    rep.saveStepAttribute( id_transformation, id_step, SOURCE_CONFIGURATION_NAME, sourceConfigurationName );

    rep.saveStepAttribute( id_transformation, id_step, "table", hiveTableName );
    rep.saveStepAttribute( id_transformation, id_step, "tableId", hiveTableId );
  }

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }

  public RuntimeTester getRuntimeTester() {
    return runtimeTester;
  }

  public RuntimeTestActionService getRuntimeTestActionService() {
    return runtimeTestActionService;
  }
}
