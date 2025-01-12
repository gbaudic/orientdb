/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.object.enhancement.field;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.object.db.BaseObjectTest;
import java.io.IOException;
import java.util.Random;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** @author diegomtassis <a href="mailto:dta@compart.com">Diego Martin Tassis</a> */
public class OObjectBinaryDataStorageTest extends BaseObjectTest {

  @Test
  public void testSaveAndLoad_BinaryFieldsSimpleRecordMapping() throws IOException {

    // setup
    this.setStrategy(ODocumentFieldHandlingStrategyFactory.SIMPLE);

    Driver hunt = new Driver();
    hunt.setName("James Hunt");
    byte[] huntUglyPicture = randomBytes(1024 * 32);
    hunt.setImageData(huntUglyPicture);

    // exercise
    Driver savedHunt = this.database.save(hunt);
    Driver loadedHunt = this.database.load(new ORecordId(savedHunt.getId()));

    // verify
    Assert.assertNotNull(savedHunt);
    Assert.assertNotNull(loadedHunt);
    Assert.assertArrayEquals(huntUglyPicture, hunt.getImageData());
    Assert.assertArrayEquals(huntUglyPicture, savedHunt.getImageData());
    Assert.assertArrayEquals(huntUglyPicture, loadedHunt.getImageData());
  }

  @Test
  public void testSaveAndLoad_BinaryFieldsSingleRecordMapping() throws IOException {

    // setup
    this.setStrategy(ODocumentFieldHandlingStrategyFactory.SINGLE_ORECORD_BYTES);

    Driver lauda = new Driver();
    lauda.setName("Niki Lauda");
    byte[] laudaRealisticPicture = randomBytes(1024 * 64);
    lauda.setImageData(laudaRealisticPicture);

    // exercise
    Driver savedLauda = this.database.save(lauda);
    Driver loadedLauda = this.database.load(new ORecordId(savedLauda.getId()));

    // verify
    Assert.assertNotNull(savedLauda);
    Assert.assertNotNull(loadedLauda);
    Assert.assertArrayEquals(laudaRealisticPicture, lauda.getImageData());
    Assert.assertArrayEquals(laudaRealisticPicture, savedLauda.getImageData());
    Assert.assertArrayEquals(laudaRealisticPicture, loadedLauda.getImageData());
  }

  @Test
  public void testSaveAndLoad_BinaryFieldsSplitRecordMapping() throws IOException {

    // setup
    this.setStrategy(ODocumentFieldHandlingStrategyFactory.SPLIT_ORECORD_BYTES);

    Driver prost = new Driver();
    prost.setName("Alain Prost");
    byte[] prostUglyPicture = randomBytes(1024 * 128 + 1);
    prost.setImageData(prostUglyPicture);

    // exercise
    Driver savedProst = this.database.save(prost);
    Driver loadedProst = this.database.load(new ORecordId(savedProst.getId()));

    // verify
    Assert.assertNotNull(savedProst);
    Assert.assertNotNull(loadedProst);
    Assert.assertArrayEquals(prostUglyPicture, prost.getImageData());
    Assert.assertArrayEquals(prostUglyPicture, savedProst.getImageData());
    Assert.assertArrayEquals(prostUglyPicture, loadedProst.getImageData());
  }

  @Test
  public void testSaveAndLoad_DefaultRecordMapping() throws IOException {

    // setup
    this.setStrategy(-1);

    Driver monzasGorilla = new Driver();
    monzasGorilla.setName("Vittorio Brambilla");
    byte[] brambillaPicture = randomBytes(1024 * 32);
    monzasGorilla.setImageData(brambillaPicture);

    // exercise
    Driver savedBrambilla = this.database.save(monzasGorilla);
    Driver loadedBrambilla = this.database.load(new ORecordId(savedBrambilla.getId()));

    // verify
    Assert.assertNotNull(savedBrambilla);
    Assert.assertNotNull(loadedBrambilla);
    Assert.assertArrayEquals(brambillaPicture, monzasGorilla.getImageData());
    Assert.assertArrayEquals(brambillaPicture, savedBrambilla.getImageData());
    Assert.assertArrayEquals(brambillaPicture, loadedBrambilla.getImageData());
  }

  @Test
  public void testSaveAndLoad_BinaryFieldsSimpleRecordMapping_InstantiatePojoUsingDbFactory()
      throws IOException {

    // setup
    this.setStrategy(ODocumentFieldHandlingStrategyFactory.SIMPLE);

    Driver ronnie = this.database.newInstance(Driver.class);
    ronnie.setName("Ronnie Peterson");
    byte[] ronniePicture = randomBytes(1024 * 32);
    ronnie.setImageData(ronniePicture);

    // exercise
    Driver savedRonnie = this.database.save(ronnie);
    Driver loadedRonnie = this.database.load(new ORecordId(savedRonnie.getId()));

    // verify
    Assert.assertNotNull(savedRonnie);
    Assert.assertNotNull(loadedRonnie);
    Assert.assertArrayEquals(ronniePicture, ronnie.getImageData());
    Assert.assertArrayEquals(ronniePicture, savedRonnie.getImageData());
    Assert.assertArrayEquals(ronniePicture, loadedRonnie.getImageData());
  }

  @Before
  public void before() {
    super.before();
    database.getEntityManager().registerEntityClass(Driver.class);
  }

  private void setStrategy(int strategy) {
    // Store strategy setting
    if (strategy >= 0) {
      OGlobalConfiguration.DOCUMENT_BINARY_MAPPING.setValue(strategy);
    }
  }

  private byte[] randomBytes(int size) {

    byte[] b = new byte[size];
    new Random().nextBytes(b);

    return b;
  }
}
