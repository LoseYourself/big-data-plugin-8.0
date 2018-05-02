/*! ******************************************************************************
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

package org.pentaho.amazon.s3;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;

import org.pentaho.di.core.xml.XMLHandler;

import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Document;

import java.util.Collections;

import static org.junit.Assert.*;

public class S3FileOutputMetaTest {
  S3FileOutputMeta meta = new S3FileOutputMeta();

  @BeforeClass
  public static void setClassUp() throws Exception {
    KettleEnvironment.init();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    KettleEnvironment.shutdown();
  }

  @Before
  public void setUp() throws Exception {
    Document doc = XMLHandler.loadXMLFile( "./src/test/resources/s3OutputMetaTest.ktr" );
    meta.loadXML( doc.getFirstChild(), Collections.<DatabaseMeta>emptyList(), (IMetaStore) null );
  }


  @Test
  public void testGetAccessKey() throws Exception {
    assertEquals( "Problem with reading Accesskey", "1", meta.getAccessKey() );
  }

  @Test
  public void testGetSecretKey() throws Exception {
    assertEquals( "Problem with reading Secretkey", "2", meta.getSecretKey() );
  }
}
