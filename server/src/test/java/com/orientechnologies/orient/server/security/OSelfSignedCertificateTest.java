package com.orientechnologies.orient.server.security;

import junit.framework.TestCase;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Date;

public class OSelfSignedCertificateTest extends TestCase {

    OSelfSignedCertificate testInstance;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        testInstance = new OSelfSignedCertificate();
        testInstance.setAlgorithm(OSelfSignedCertificate.DEFAULT_CERTIFICATE_ALGORITHM);
        testInstance.setCertificate_name(OSelfSignedCertificate.DEFAULT_CERTIFICATE_NAME);
        testInstance.setCertificate_SN(1701198707);
        testInstance.setCertificate_pwd(null);
        testInstance.setKey_size(OSelfSignedCertificate.DEFAULT_CERTIFICATE_KEY_SIZE);
        testInstance.setOwner_FDN(OSelfSignedCertificate.DEFAULT_CERTIFICATE_OWNER);
        testInstance.setValidity(OSelfSignedCertificate.DEFAULT_CERTIFICATE_VALIDITY);
    }

    @Test
    public void testSetUnsuitableSerialNumber() throws Exception {
        try {
            testInstance.setCertificate_SN(0);
        }catch(SwitchToDefaultParamsException e){
            return;
        }
        Assert.fail();
    }

    @Test
    public void testGenerateCertificateKeyPair() throws Exception {
        testInstance.generateCertificateKeyPair();
    }

    @Test
    public void testComposeSelfSignedCertificate() throws Exception {
        testInstance.generateCertificateKeyPair();
        testInstance.composeSelfSignedCertificate();
        testInstance.checkThisCertificate();
    }

    @Test
    public void testCheckValidityPerid() throws Exception {
        testComposeSelfSignedCertificate();

        Assert.assertThrows(CertificateNotYetValidException.class,()-> {
            X509Certificate cert = testInstance.getCertificate();
            PublicKey pubK = testInstance.getPublicKey();
            Date yesterday = new Date(System.currentTimeMillis()-86400000);
            OSelfSignedCertificate.checkCertificate(cert, pubK, yesterday);
        });
    }

    @Test
    public void testCertificateSignatureAgainstTamperPublicKey() throws Exception {
        testComposeSelfSignedCertificate();

        Assert.assertThrows(SignatureException.class,()-> {
            X509Certificate cert = testInstance.getCertificate();
            KeyPair tamperK = OSelfSignedCertificate.computeKeyPair(testInstance.getAlgorithm(),testInstance.getKey_size());
            Date yesterday = new Date(System.currentTimeMillis());
            OSelfSignedCertificate.checkCertificate(cert, tamperK.getPublic(), yesterday);
        });
    }

//    @Test
//    public void testSaveCertificate() {
//
//    }

}