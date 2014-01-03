package com.amazon.dse.discovery.dns;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;

import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazon.dse.discovery.DirectoryServiceLookupInterface;
import com.amazon.dse.discovery.LookupException;
import com.amazon.dse.discovery.RegistryEntryRecord;

/**
 * The Registry implementation that reads from Amazon's DNS system<br>
 */
public class DNSServiceLookup extends DirectoryServiceLookupInterface {

	private static final Log log = LogFactory.getLog(DNSServiceLookup.class);

	private static final int MAX_DNS_LABEL_LENGTH = 63;

	private DirContext getJNDIContext() throws NamingException {

		/*
		 * The correct way to resolve a directory service is from
		 * /etc/resolv.conf (nameserver) Currently our prod hosts in DCs use an
		 * anycast address and FCs may have different dns servers Using the
		 * defaults lets JNDI to pick the correct server from resolv.conf
		 */

		String providerUrl = System.getProperty("dns.readAddress", "dns:");
		log.debug("DNS read address is " + providerUrl);

		Hashtable<String, String> env = new Hashtable<String, String>();
		env.put("java.naming.factory.initial",
				"com.sun.jndi.dns.DnsContextFactory");
		env.put("java.naming.provider.url", providerUrl);
		return new InitialDirContext(env);
	}

	/**
	 * Verifies that the individual components of the specified key are less
	 * than MAX_DNS_LABEL_LENGTH characters
	 * 
	 * @param key
	 *            A DNS entry whose SRV records we are interested in
	 */

	private void verifyKeyComponentLength(String key) {

		if (key == null) {
			throw new IllegalArgumentException("Specified key is null");
		}

		String[] tokens = key.split("\\.");

		for (String s : tokens) {
			if (s.length() > MAX_DNS_LABEL_LENGTH) {
				throw new IllegalArgumentException("Specified Key exceeds "
						+ MAX_DNS_LABEL_LENGTH + " characters: " + s);
			}
		}
	}

	/**
	 * @param key
	 *            A DNS entry whose SRV records we are interested in
	 * @return List (possibly empty) of <code>RegistryEntryRecord</code>
	 *         associated with the <code> clusterName </code>
	 */

	@Override
	public List<RegistryEntryRecord> lookup(String key) throws LookupException {

		ArrayList<RegistryEntryRecord> retVal = new ArrayList<RegistryEntryRecord>();

		verifyKeyComponentLength(key);

		Attribute srvRecords = null;

		DirContext ctx;
		try {
			ctx = getJNDIContext();
		} catch (NamingException e) {
			throw new LookupException("Exception looking up: " + key, e);
		}

		Attributes dnsReply;
		try {
			dnsReply = ctx.getAttributes(key, new String[] { "SRV" });
		} catch (NameNotFoundException e0) {
			// Since we cannot get an empty response from getAttributes but only
			// an exception, we specifically check for it
			return retVal;
		} catch (NamingException e1) {
			throw new LookupException("Exception looking up: " + key, e1);
		} 
		
		srvRecords = dnsReply.get("SRV");

		if (srvRecords == null) {
			log.warn("There are no SRV records associated with the cluster name: " + key);
			return retVal;
		}

		for (int i = 0; i < srvRecords.size(); i++) {

			String s;
			try {
				s = (String) srvRecords.get(i);
			} catch (NamingException e) {
				throw new LookupException("DNS lookup error for:  " + key, e);
			}

			if (s == null)
				break;

			log.debug("Found Entry: " + s);
			Scanner scanStr = new Scanner(s); // default whitespace
			// delimiter works
			// fine

			if (s.indexOf("amazon.com") >= 0) {

				RegistryEntryRecord rec = new RegistryEntryRecord();

				@SuppressWarnings("unused")
				int unused;
				unused = scanStr.nextInt(); // priority
				unused = scanStr.nextInt(); // weight

				rec.setPort(scanStr.nextInt());

				String hostName = scanStr.next();
				if (hostName.endsWith(".")) {
					// Strip off the trailing period
					hostName = hostName.substring(0, hostName.length() - 1);
				}

				rec.setName(hostName);

				retVal.add(rec);
			}

			scanStr.close();
		}
		return retVal;
	}

}
