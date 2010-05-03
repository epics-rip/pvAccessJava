/*
 * Copyright (c) 2004 by Cosylab
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

package org.epics.ca.client.impl.remote.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import org.epics.ca.impl.remote.ConnectionException;
import org.epics.ca.impl.remote.Connector;
import org.epics.ca.impl.remote.Context;
import org.epics.ca.impl.remote.ProtocolType;
import org.epics.ca.impl.remote.ResponseHandler;
import org.epics.ca.impl.remote.Transport;
import org.epics.ca.impl.remote.TransportClient;
import org.epics.ca.util.sync.NamedLockPattern;

/**
 * Channel Access TCP connector.
 * @author <a href="mailto:matej.sekoranjaATcosylab.com">Matej Sekoranja</a>
 * @version $Id$
 */
public class BlockingTCPConnector implements Connector {

	/**
	 * Context instance.
	 */
	private Context context;
	
	/**
	 * Context instance.
	 */
	private NamedLockPattern namedLocker;

	/**
	 * Context instance.
	 */
	private static final int LOCK_TIMEOUT = 20 * 1000;	// 20s

	/**
	 * Receive buffer size.
	 */
	private int receiveBufferSize;
	
	/**
	 * Beacon interval.
	 */
	private float beaconInterval; 

	/**
	 * @param context
	 */
	public BlockingTCPConnector(Context context, int receiveBufferSize, float beaconInterval) {
		this.context = context;
		this.receiveBufferSize = receiveBufferSize;
		this.beaconInterval = beaconInterval;
		namedLocker = new NamedLockPattern();
	}
	
	
	/**
	 * @see org.epics.ca.impl.remote.Connector#connect(org.epics.ca.impl.remote.TransportClient, org.epics.ca.impl.remote.ResponseHandler, java.net.InetSocketAddress, short, short)
	 */
	public Transport connect(TransportClient client, ResponseHandler responseHandler,
							 InetSocketAddress address, short transportRevision, short priority)
		throws ConnectionException
	{

		SocketChannel socket = null;
		
		// first try to check cache w/o named lock...
		BlockingClientTCPTransport transport = (BlockingClientTCPTransport)context.getTransportRegistry().get(ProtocolType.TCP.name(), address, priority);
		if (transport != null)
		{
			context.getLogger().finer("Reusing existant connection to CA server: " + address);
			if (transport.acquire(client))
				return transport;
		}

		boolean lockAcquired = namedLocker.acquireSynchronizationObject(address, LOCK_TIMEOUT);
		if (lockAcquired)
		{ 
			try
			{   
				// ... transport created during waiting in lock 
				transport = (BlockingClientTCPTransport)context.getTransportRegistry().get(ProtocolType.TCP.name(), address, priority);
				if (transport != null)
				{
					context.getLogger().finer("Reusing existant connection to CA server: " + address);
					if (transport.acquire(client))
						return transport;
				}
				     
				context.getLogger().finer("Connecting to CA server: " + address);
				
				socket = tryConnect(address, 3);

				// use blocking channel
				socket.configureBlocking(true);
			
				// enable TCP_NODELAY (disable Nagle's algorithm)
				socket.socket().setTcpNoDelay(true);
				
				// enable TCP_KEEPALIVE
				socket.socket().setKeepAlive(true);
			
				// TODO tune buffer sizes?! Win32 defaults are 8k, which is OK
				//socket.socket().setReceiveBufferSize();
				//socket.socket().setSendBufferSize();
	
				// create transport
				transport = new BlockingClientTCPTransport(context, socket, responseHandler, receiveBufferSize, client, transportRevision, beaconInterval, priority);

				// verify
				if (!transport.waitUntilVerified(3000))
				{
					transport.close(true);
					context.getLogger().finer("Connection to CA client " + address + " failed to be validated, closing it.");
					throw new ConnectionException("Failed to verify connection to '" + address + "'.", address, ProtocolType.TCP.name(), null);
				}
				
				// TODO send security token
				
				context.getLogger().finer("Connected to CA server: " + address);
	
				return transport;
			}
			catch (Throwable th)
			{
				// close socket, if open
				try
				{
					if (socket != null)
						socket.close();
				}
				catch (Throwable t) { /* noop */ }
	
				throw new ConnectionException("Failed to connect to '" + address + "'.", address, ProtocolType.TCP.name(), th);
			}
			finally
			{
				namedLocker.releaseSynchronizationObject(address);	
			}
		}
		else
		{     
			throw new ConnectionException("Failed to obtain synchronization lock for '" + address + "', possible deadlock.", address, ProtocolType.TCP.name(), null);
		}
	}

	/**
	 * Tries to connect to the given adresss.
	 * @param address
	 * @param tries
	 * @return
	 * @throws IOException
	 */
	private SocketChannel tryConnect(InetSocketAddress address, int tries)
		throws IOException
	{
		
		IOException lastException = null;
				
		for (int tryCount = 0; tryCount < tries; tryCount++)
		{

			// sleep for a while
			if (tryCount > 0)
			{
				try {
					Thread.sleep(100);
				} catch (InterruptedException ie) {}
			}

			context.getLogger().finest("Openning socket to CA server " + address + ", attempt " + (tryCount+1) + ".");

			try
			{
				return SocketChannel.open(address);
			}
			catch (IOException ioe)
			{
				lastException = ioe;
			}


		}

		throw lastException;
	}

}