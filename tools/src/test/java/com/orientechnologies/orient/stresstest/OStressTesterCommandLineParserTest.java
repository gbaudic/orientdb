package com.orientechnologies.orient.stresstest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import org.junit.Test;

public class OStressTesterCommandLineParserTest {

  @Test
  public void testCommandLineArgs() throws Exception {
    try {
      OStressTesterCommandLineParser.getStressTester(new String[] {""});
      fail();
    } catch (Exception ex) {
      assertTrue(
          ex.getMessage()
              .contains(
                  String.format(
                      OStressTesterCommandLineParser.COMMAND_LINE_PARSER_INVALID_OPTION, "")));
    }

    try {
      OStressTesterCommandLineParser.getStressTester(new String[] {"-i foo"});
      fail();
    } catch (Exception ex) {
      assertTrue(
          ex.getMessage()
              .contains(
                  String.format(
                      OStressTesterCommandLineParser.COMMAND_LINE_PARSER_EXPECTED_VALUE,
                      "-i foo")));
    }

    try {
      OStressTesterCommandLineParser.getStressTester(new String[] {"-i"});
      fail();
    } catch (Exception ex) {
      assertTrue(
          ex.getMessage()
              .contains(
                  String.format(
                      OStressTesterCommandLineParser.COMMAND_LINE_PARSER_EXPECTED_VALUE, "-i")));
    }

    try {
      OStressTesterCommandLineParser.getStressTester(new String[] {"-c", "10", "-n"});
      fail();
    } catch (Exception ex) {
      assertTrue(
          ex.getMessage()
              .contains(
                  String.format(
                      OStressTesterCommandLineParser.COMMAND_LINE_PARSER_EXPECTED_VALUE, "-n")));
    }

    try {
      OStressTesterCommandLineParser.getStressTester(new String[] {"-m", "foo"});
      fail();
    } catch (Exception ex) {
      assertTrue(
          ex.getMessage()
              .contains(
                  String.format(
                      OStressTesterCommandLineParser.COMMAND_LINE_PARSER_INVALID_MODE, "foo")));
    }

    try {
      OStressTesterCommandLineParser.getStressTester(new String[] {"-m", "remote"});
      fail();
    } catch (Exception ex) {
      assertTrue(
          ex.getMessage()
              .contains(OStressTesterCommandLineParser.COMMAND_LINE_PARSER_MISSING_REMOTE_IP));
    }

    try {
      OStressTesterCommandLineParser.getStressTester(new String[] {"-tx", "10"});
      fail();
    } catch (Exception ex) {
      assertTrue(
          ex.getMessage()
              .contains(OStressTesterCommandLineParser.COMMAND_LINE_PARSER_MODE_PARAM_MANDATORY));
    }

    OStressTester stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {"--root-password", "foo", "-m", "plocal"});
    assertEquals("foo", stressTester.getPassword());
    assertEquals(OStressTester.OMode.PLOCAL, stressTester.getSettings().mode);
    assertNull(stressTester.getSettings().plocalPath);

    stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {"-m", "memory", "--root-password", "foo"});
    assertEquals("foo", stressTester.getPassword());
    assertEquals(OStressTester.OMode.MEMORY, stressTester.getSettings().mode);

    stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {"-c", "4", "--root-password", "foo", "-m", "plocal"});
    assertEquals(4, stressTester.getThreadsNumber());
    assertNull(stressTester.getSettings().remoteIp);
    assertEquals(2424, stressTester.getSettings().remotePort);

    stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {"-m", "remote", "--remote-ip", "127.0.0.1", "--root-password", "foo"});
    assertEquals("foo", stressTester.getPassword());
    assertEquals("127.0.0.1", stressTester.getSettings().remoteIp);
    assertEquals(2424, stressTester.getSettings().remotePort);

    stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {
              "-m",
              "remote",
              "--remote-ip",
              "127.0.0.1",
              "--root-password",
              "foo",
              "--remote-port",
              "1025"
            });
    assertEquals("foo", stressTester.getPassword());
    assertEquals("127.0.0.1", stressTester.getSettings().remoteIp);
    assertEquals(1025, stressTester.getSettings().remotePort);

    String tmpDir = System.getProperty("java.io.tmpdir") + "/orientdb/";
    if (tmpDir.endsWith(File.separator)) {
      tmpDir = tmpDir.substring(0, tmpDir.length() - File.separator.length());
    }

    stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {"-c", "4", "-m", "plocal", "--root-password", "foo", "-d", tmpDir});
    assertEquals(4, stressTester.getThreadsNumber());
    assertEquals(OStressTester.OMode.PLOCAL, stressTester.getMode());
    assertEquals("foo", stressTester.getPassword());
    assertEquals(tmpDir, stressTester.getSettings().plocalPath);

    stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {
              "-c",
              "4",
              "-m",
              "plocal",
              "--root-password",
              "foo",
              "-tx",
              "12",
              "-d",
              tmpDir + File.separator
            });
    assertEquals(4, stressTester.getThreadsNumber());
    assertEquals(OStressTester.OMode.PLOCAL, stressTester.getMode());
    assertEquals("foo", stressTester.getPassword());
    assertEquals(12, stressTester.getTransactionsNumber());
    assertEquals(tmpDir, stressTester.getSettings().plocalPath);

    stressTester =
        OStressTesterCommandLineParser.getStressTester(
            new String[] {
              "-c", "4", "-m", "plocal", "-w", "crud:c1r1u1d1", "--root-password", "foo"
            });
    assertEquals(4, stressTester.getThreadsNumber());
    assertEquals(OStressTester.OMode.PLOCAL, stressTester.getMode());
    assertEquals("foo", stressTester.getPassword());

    // TODO: add tests for checking value of creates/reads to check the remotion of iteration had no
    // impact
  }
}
