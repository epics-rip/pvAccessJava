/*
 * Copyright (c) 2009 by Cosylab
 *
 * The full license specifying the redistribution, modification, usage and other
 * rights and obligations is included with the distribution of this project in
 * the file "LICENSE-CAJ". If the license is not included visit Cosylab web site,
 * <http://www.cosylab.com>.
 *
 * THIS SOFTWARE IS PROVIDED AS-IS WITHOUT WARRANTY OF ANY KIND, NOT EVEN THE
 * IMPLIED WARRANTY OF MERCHANTABILITY. THE AUTHOR OF THIS SOFTWARE, ASSUMES
 * _NO_ RESPONSIBILITY FOR ANY CONSEQUENCE RESULTING FROM THE USE, MODIFICATION,
 * OR REDISTRIBUTION OF THIS SOFTWARE.
 */

package org.epics.ca.impl.remote.utils;

import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.epics.ca.CAException;
import org.epics.ca.client.ClientContext;
import org.epics.ca.client.impl.remote.BeaconHandler;
import org.epics.ca.client.impl.remote.ClientContextImpl;
import org.epics.pvData.property.TimeStamp;
import org.epics.pvData.pv.PVField;

/**
 * Simple server monitor GUI.
 * @author msekoranja
 * @version $Id$
 */
public class ServerStatusMonitor implements BeaconHandler {

	/**
	 * Context implementation.
	 */
	private static class BeaconMonitorContextImpl extends ClientContextImpl
	{
		/**
		 * Beacon handler.
		 */
		private BeaconHandler beaconHandler;
		
		/**
		 * Constructor.
		 * @param beaconHandler handler used to handle beacon messages. 
		 */
		public BeaconMonitorContextImpl(BeaconHandler beaconHandler) {
			super();
			this.beaconHandler = beaconHandler;
		}

		/**
		 * @see org.epics.ca.client.impl.remote.ClientContextImpl#getBeaconHandler(java.net.InetSocketAddress)
		 */
		@Override
		public BeaconHandler getBeaconHandler(InetSocketAddress responseFrom) {
			return beaconHandler;
		}
		
	}

	/**
	 * ISO 8601 date formatter.
	 */
	private static SimpleDateFormat timeFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

	/* (non-Javadoc)
	 * @see org.epics.ca.client.impl.remote.BeaconHandler#beaconNotify(InetSocketAddress, byte, long, TimeStamp, int, PVField)
	 */
	public void beaconNotify(InetSocketAddress from, byte remoteTransportRevision,
							 long timestamp, TimeStamp startupTime, int sequentalID,
							 PVField data) {
		final byte major = (byte)(remoteTransportRevision >> 4); 
		final byte minor = (byte)(remoteTransportRevision & 0x0F);
		System.out.printf("[%s] %s: seqID %d, version %d.%d, startup %s\n",
				timeFormatter.format(new Date(timestamp)),
				from,
				sequentalID, major, minor,
				timeFormatter.format(new Date(startupTime.getMilliSeconds())));
		if (data != null)
			System.out.println(data);
	}


    /**
     * CA context.
     */
    private ClientContext context = null;
    
    /**
     * Initialize JCA context.
     * @throws CAException	throws on any failure.
     */
    private void initialize() throws CAException {
        
		// Create a context with default configuration values.
		context = new BeaconMonitorContextImpl(this);
		context.initialize();

		// Display basic information about the context.
        System.out.println(context.getVersion().getVersionString());
        context.printInfo(); System.out.println();
    }

    /**
     * Destroy JCA context.
     */
    public void destroy() {
        
        try {

            // Destroy the context, check if never initialized.
            if (context != null)
                context.destroy();
            
        } catch (Throwable th) {
            th.printStackTrace();
        }
    }
    
	/**
	 * Do the work...
	 */
	public void execute() {

		try {
		    
		    // initialize context
		    initialize();
		    
		} catch (Throwable th) {
			th.printStackTrace();
		}

	}
	
	
	/**
	 * Program entry point. 
	 * @param args	command-line arguments
	 */
	public static void main(String[] args) {
		// execute
		new ServerStatusMonitor().execute();
	}

}