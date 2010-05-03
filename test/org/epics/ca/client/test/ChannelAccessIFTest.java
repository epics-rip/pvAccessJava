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

package org.epics.ca.client.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.epics.ca.CAConstants;
import org.epics.ca.client.Channel;
import org.epics.ca.client.ChannelArray;
import org.epics.ca.client.ChannelArrayRequester;
import org.epics.ca.client.ChannelFind;
import org.epics.ca.client.ChannelFindRequester;
import org.epics.ca.client.ChannelGet;
import org.epics.ca.client.ChannelGetRequester;
import org.epics.ca.client.ChannelProcess;
import org.epics.ca.client.ChannelProcessRequester;
import org.epics.ca.client.ChannelProvider;
import org.epics.ca.client.ChannelPut;
import org.epics.ca.client.ChannelPutGet;
import org.epics.ca.client.ChannelPutGetRequester;
import org.epics.ca.client.ChannelPutRequester;
import org.epics.ca.client.ChannelRequester;
import org.epics.ca.client.GetFieldRequester;
import org.epics.ca.client.Channel.ConnectionState;
import org.epics.pvData.factory.ConvertFactory;
import org.epics.pvData.factory.PVDataFactory;
import org.epics.pvData.misc.BitSet;
import org.epics.pvData.monitor.Monitor;
import org.epics.pvData.monitor.MonitorElement;
import org.epics.pvData.monitor.MonitorRequester;
import org.epics.pvData.property.TimeStamp;
import org.epics.pvData.property.TimeStampFactory;
import org.epics.pvData.pv.Convert;
import org.epics.pvData.pv.DoubleArrayData;
import org.epics.pvData.pv.Field;
import org.epics.pvData.pv.MessageType;
import org.epics.pvData.pv.PVArray;
import org.epics.pvData.pv.PVDataCreate;
import org.epics.pvData.pv.PVDouble;
import org.epics.pvData.pv.PVDoubleArray;
import org.epics.pvData.pv.PVField;
import org.epics.pvData.pv.PVInt;
import org.epics.pvData.pv.PVString;
import org.epics.pvData.pv.PVStringArray;
import org.epics.pvData.pv.PVStructure;
import org.epics.pvData.pv.ScalarType;
import org.epics.pvData.pv.Status;
import org.epics.pvData.pv.StringArrayData;
import org.epics.pvData.pv.Structure;
import org.epics.pvData.pv.Type;
import org.epics.pvData.pv.Status.StatusType;
import org.epics.pvData.pvCopy.PVCopyFactory;

/**
 * Channel Access IF test.
 * @author <a href="mailto:matej.sekoranjaATcosylab.com">Matej Sekoranja</a>
 * @version $Id$
 */
public abstract class ChannelAccessIFTest extends TestCase {

	/**
	 * Get channel provider to be tested.
	 * @return the channel provider to be tested.
	 */
	public abstract ChannelProvider getChannelProvider();

	/**
	 * Get timeout value (in ms).
	 * @return the timeout value.
	 */
	public abstract long getTimeoutMs();

	/**
	 * Is local CA implementation.
	 * @return local or not.
	 */
	public abstract boolean isLocal();
	
	protected Set<Channel> channels = new HashSet<Channel>();
	
	/**
	 * If provider cannot be destroyed,
	 * channel must be registered so that is destroyed by tearDown().
	 * @param channel
	 */
	public void registerChannelForDestruction(Channel channel)
	{
		if (channel == null)
			return;
		
		synchronized (channels) {
			channels.add(channel);
		}
	};
	
	/* (non-Javadoc)
	 * @see junit.framework.TestCase#tearDown()
	 */
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		synchronized (channels) {
			for (Channel channel : channels) {
				try {
					if (channel.getConnectionState() != ConnectionState.DESTROYED)
						channel.destroy();
				} catch (Throwable th) { th.printStackTrace(); }
			}
			channels.clear();
		}
	}

	public void testTestImpl()
	{
		assertNotNull(getChannelProvider());
		assertSame("getChannelProvider() returns different instance for each call; fix the test",
				getChannelProvider(), getChannelProvider());
		assertTrue("getTimeoutMs() <= 0", getTimeoutMs() > 0);
	}
	
	public void testProviderName()
	{
		final ChannelProvider provider = getChannelProvider();
		
		assertNotNull(provider.getProviderName());
		assertSame("getProviderName() returns different String instace for each call", provider.getProviderName(), provider.getProviderName());
		assertTrue("empty getProviderName()", provider.getProviderName().length() > 0);
	}
	
	protected static class ChannelRequesterCreatedTestImpl implements ChannelRequester {
		
		public Channel channel;
		public Status status;
		public int createdCount = 0;
		public int stateChangeCount = 0;
		
		@Override
		public void message(String message, MessageType messageType) {
			System.err.println("[" + messageType + "] " + message);
		}
		
		@Override
		public String getRequesterName() {
			return this.getClass().getName();
		}
		
		@Override
		public synchronized void channelStateChange(Channel c, ConnectionState connectionState) {
			stateChangeCount++;
			this.notifyAll();
		}
		
		@Override
		public synchronized void channelCreated(Status status, Channel channel) {
			createdCount++;
			this.status = status;
			this.channel = channel;
			this.notifyAll();
		}
	};

	protected static class ChannelFindRequesterTestImpl implements ChannelFindRequester {
		
		public Status status;
		public boolean wasFound;
		public int findCount = 0;

		@Override
		public synchronized void channelFindResult(Status status, ChannelFind channelFind, boolean wasFound) {
			findCount++;
			this.status = status;
			this.wasFound = wasFound;
			this.notifyAll();
		}
	};

	public void testCreateChannel() throws Throwable
	{
		final ChannelProvider provider = getChannelProvider();

		//
		// null requester test, exception expected
		//
		try
		{
			provider.createChannel("someName", null, ChannelProvider.PRIORITY_DEFAULT);
			fail("null ChannelRequester accepted");
		} catch (IllegalArgumentException th) {
			// OK
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}
		
		Channel channel;
		
		//
		// null channel name
		//
		ChannelRequesterCreatedTestImpl crcti = new ChannelRequesterCreatedTestImpl();
		try
		{
			provider.createChannel(null, crcti, ChannelProvider.PRIORITY_DEFAULT);
			fail("null channel name accepted");
		} catch (IllegalArgumentException th) {
			// OK
			synchronized (crcti) {
				assertEquals(0, crcti.createdCount);
			}
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}

		//
		// invalid priority value
		//
		crcti = new ChannelRequesterCreatedTestImpl();
		try
		{
			provider.createChannel("counter", crcti, (short)(ChannelProvider.PRIORITY_MIN - 1));
			fail("invalid priority accepted");
		} catch (IllegalArgumentException th) {
			// OK
			synchronized (crcti) {
				assertEquals(0, crcti.createdCount);
			}
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}

		//
		// finally, create that channel
		//
		crcti = new ChannelRequesterCreatedTestImpl();
		synchronized (crcti) {
			channel = provider.createChannel("counter", crcti, (short)(ChannelProvider.PRIORITY_DEFAULT + 1));
			registerChannelForDestruction(channel);
			assertNotNull(channel);
			
			if (crcti.createdCount == 0)
				crcti.wait(getTimeoutMs());
			
			assertEquals(1, crcti.createdCount);
			assertTrue(crcti.status.isSuccess());
			assertNotNull(crcti.channel);
			
			// let's be mean and wait for some time
			crcti.wait(1000);

			assertEquals(1, crcti.createdCount);
		}
		
		// local CA does not recreate provider, so we don't want to destroy it
		if (isLocal())
			return;
		
		provider.destroy();

		//
		// request on destroyed provider
		//
		crcti = new ChannelRequesterCreatedTestImpl();
		synchronized (crcti) {
			channel = provider.createChannel("counter", crcti, ChannelProvider.PRIORITY_DEFAULT);
			registerChannelForDestruction(channel);
			assertNull(channel);
			
			if (crcti.createdCount == 0)
				crcti.wait(getTimeoutMs());
			
			assertEquals(1, crcti.createdCount);
			assertFalse(crcti.status.isSuccess());
			assertNull(crcti.channel);
		}
	}

	
	public void testFindChannel() throws Throwable
	{
		final ChannelProvider provider = getChannelProvider();

		//
		// null requester test, exception expected
		//
		try
		{
			provider.channelFind("someName", null);
			fail("null ChannelFindRequester accepted");
		} catch (IllegalArgumentException th) {
			// OK
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}
		
		ChannelFind channelFind;
		
		//
		// null channel name
		//
		ChannelFindRequesterTestImpl cfrti = new ChannelFindRequesterTestImpl();
		try {
			channelFind = provider.channelFind(null, cfrti);
			fail("null channel name accepted");
		} catch (IllegalArgumentException th) {
			// OK
			synchronized (cfrti) {
				assertEquals(0, cfrti.findCount);
			}
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}
		
		//
		// finally, find that channel
		//
		cfrti = new ChannelFindRequesterTestImpl();
		synchronized (cfrti) {
			channelFind = provider.channelFind("counter", cfrti);
			assertNotNull(channelFind);
			
			if (cfrti.findCount == 0)
				cfrti.wait(getTimeoutMs());
			
			assertEquals(1, cfrti.findCount);
			assertTrue(cfrti.status.isSuccess());
			assertTrue(cfrti.wasFound);
			
			// let's be mean and wait for some time
			cfrti.wait(1000);

			assertEquals(1, cfrti.findCount);
		}

		
		//
		// finally, find non-existing channel
		//
		int count;
		cfrti = new ChannelFindRequesterTestImpl();
		synchronized (cfrti) {
			channelFind = provider.channelFind("nonExisting", cfrti);
			assertNotNull(channelFind);
			
			if (cfrti.findCount == 0)
				cfrti.wait(getTimeoutMs());
			
			// 2 possible scenarios
	
			// 1. immediate response (local CA)
			count = cfrti.findCount;
			if (count == 1)
			{
				assertTrue(cfrti.status.isSuccess());
				assertFalse(cfrti.wasFound);
			}
			// 2. no response yet
			else if (count == 0)
			{
				// nothing to test
			}
			else
				fail("invalid count on channelFindResult() call");
			
			// let's be mean and wait for some time
			cfrti.wait(1000);
			
			assertEquals(count, cfrti.findCount);
		}

		// cancel...
		channelFind.cancelChannelFind();

		// no more responses
		synchronized (cfrti) {
			// let's be mean and wait for some time
			cfrti.wait(1000);
			
			assertEquals(count, cfrti.findCount);
		}
		
		// local CA does not recreate provider, so we don't want to destroy it
		if (isLocal())
			return;

		provider.destroy();

		//
		// request on destroyed provider
		//
		cfrti = new ChannelFindRequesterTestImpl();
		synchronized (cfrti) {
			channelFind = provider.channelFind("nonExisting", cfrti);
			assertNull(channelFind);
			
			if (cfrti.findCount == 0)
				cfrti.wait(getTimeoutMs());
			
			assertEquals(1, cfrti.findCount);
			assertFalse(cfrti.status.isSuccess());
			assertFalse(cfrti.wasFound);
		}
	}


	public void testQuery() throws Throwable
	{
		final ChannelProvider provider = getChannelProvider();

		//
		// null requester test, exception expected
		//
		try
		{
			PVField nullQuery = pvDataCreate.createPVScalar(null, "value", ScalarType.pvDouble);
			provider.query(nullQuery, null);
			fail("null ChannelQueryRequester accepted");
		} catch (Throwable th) {
			// OK
		}
		
		// TODO other functionality not yet tested (since not implemented)
	}
	
	public void testChannel() throws Throwable
	{
		final ChannelProvider provider = getChannelProvider();

		Channel channel;
		ChannelRequesterCreatedTestImpl crcti = new ChannelRequesterCreatedTestImpl();
		synchronized (crcti) {
			channel = provider.createChannel("counter", crcti, (short)(ChannelProvider.PRIORITY_DEFAULT + 1));
			registerChannelForDestruction(channel);
			assertNotNull(channel);
			
			if (crcti.createdCount == 0)
				crcti.wait(getTimeoutMs());
			
			assertEquals("channelCreated not called", 1, crcti.createdCount);
			assertTrue(crcti.stateChangeCount <= 1);	
			assertTrue(crcti.status.isSuccess());
			assertNotNull(crcti.channel);
		}

		// test accessors
		assertEquals("counter", channel.getChannelName());
		// TODO assertEquals((short)(ChannelProvider.PRIORITY_DEFAULT + 1), channel.getChannelPriority());
		assertSame(crcti, channel.getChannelRequester());
		assertEquals(crcti.getRequesterName(), channel.getRequesterName());
		assertSame(provider, channel.getProvider());

		/*
		// not yet connected
		assertNull(channel.getRemoteAddress());
		assertFalse(channel.isConnected());
		assertEquals(ConnectionState.NEVER_CONNECTED, channel.getConnectionState());
		
		// disconnect on never connected test
		channel.disconnect();
		// state should not change at all
		// not yet connected
		assertNull(channel.getRemoteAddress());
		assertFalse(channel.isConnected());
		assertEquals(ConnectionState.NEVER_CONNECTED, channel.getConnectionState());
		synchronized (crcti) {
			assertEquals(0, crcti.stateChangeCount);
		}
		*/
		
		// now we really connect
		synchronized (crcti) {
			//channel.connect();

			// wait if not connected
			if (crcti.stateChangeCount == 0)
				crcti.wait(getTimeoutMs());
			
			assertEquals(1, crcti.createdCount);
			assertEquals("channel failed to connect (or no connect notification via channelStateChange)", 1, crcti.stateChangeCount);
			assertTrue(crcti.status.isSuccess());
		}
		assertNotNull(channel.getRemoteAddress());
		/*String channelRemoteAddress =*/ channel.getRemoteAddress();
		assertTrue(channel.isConnected());
		assertEquals(ConnectionState.CONNECTED, channel.getConnectionState());
		/*
		// duplicate connect, should be noop
		synchronized (crcti) {
			channel.connect();

			// wait for new event (failure)... or timeoutMs
			if (crcti.stateChangeCount == 1)
				crcti.wait(getTimeoutMs());
			
			assertEquals(1, crcti.createdCount);
			assertEquals(1, crcti.stateChangeCount);
			assertTrue(crcti.status.isSuccess());
		}
		assertEquals(channelRemoteAddress, channel.getRemoteAddress());
		assertTrue(channel.isConnected());
		assertEquals(ConnectionState.CONNECTED, channel.getConnectionState());
		
		
		// now we disconnect
		synchronized (crcti) {
			channel.disconnect();

			// wait for new event (failure)... or timeoutMs
			if (crcti.stateChangeCount == 1)
				crcti.wait(getTimeoutMs());
			
			assertEquals(1, crcti.createdCount);
			assertEquals("no disconnect notification via channelStateChange", 2, crcti.stateChangeCount);
			assertTrue(crcti.status.isSuccess());
		}
		assertNull(channel.getRemoteAddress());
		assertFalse(channel.isConnected());
		assertEquals(ConnectionState.DISCONNECTED, channel.getConnectionState());
		
		
		// double disconnect
		synchronized (crcti) {
			channel.disconnect();

			// wait for new event (failure)... or timeoutMs
			if (crcti.stateChangeCount == 2)
				crcti.wait(getTimeoutMs());
			
			assertEquals(1, crcti.createdCount);
			assertEquals(2, crcti.stateChangeCount);
			assertTrue(crcti.status.isSuccess());
		}
		assertNull(channel.getRemoteAddress());
		assertFalse(channel.isConnected());
		assertEquals(ConnectionState.DISCONNECTED, channel.getConnectionState());
		*/

		

		// ... and finally we destroy
		synchronized (crcti) {
			channel.destroy();

			// wait for new event (failure)... or timeoutMs
			if (crcti.stateChangeCount == 1)
			//if (crcti.stateChangeCount == 2)
				crcti.wait(getTimeoutMs());
			
			assertEquals(1, crcti.createdCount);
			// disconnected might be called first
			assertTrue("no destroy notification via channelStateChange", 2 <= crcti.stateChangeCount);
			//assertTrue("no destroy notification via channelStateChange", 3 <= crcti.stateChangeCount);
			assertTrue(crcti.status.isSuccess());
		}
//		assertNull(channel.getRemoteAddress());
		assertFalse(channel.isConnected());
		assertEquals(ConnectionState.DESTROYED, channel.getConnectionState());

		final int count = crcti.stateChangeCount;
		
		// of course, we do destroy-again test
		synchronized (crcti) {
			// noop expected
			channel.destroy();

			// wait for new event (failure)... or timeoutMs
			if (crcti.stateChangeCount == count)
				crcti.wait(getTimeoutMs());
			
			assertEquals(1, crcti.createdCount);
			// we allow warning message
			if (crcti.status.getType() != StatusType.WARNING)
				assertEquals(count, crcti.stateChangeCount);
		}
//		assertNull(channel.getRemoteAddress());
		assertFalse(channel.isConnected());
		assertEquals(ConnectionState.DESTROYED, channel.getConnectionState());
		
		// just send a message...
		channel.message("testing 1, 2, 3...", MessageType.info);
	}

	public void testChannelAccessRights() {
		// TODO null or not implemented exception
		//channel.getAccessRights(pvField);
	}
	
	
	/** ----------------------- REQUEST TESTS  -----------------------**/
	
    private class ConnectionListener implements ChannelRequester
    {
    	private Boolean notification = null;
    	
 		/* (non-Javadoc)
		 * @see org.epics.ca.client.ChannelRequester#channelCreated(org.epics.pvData.pv.Status, org.epics.ca.client.Channel)
		 */
		@Override
		public void channelCreated(Status status,
				org.epics.ca.client.Channel channel) {
			/*
			if (status.isSuccess())
				channel.connect();
			else {
			*/
			if (!status.isSuccess()) {
				synchronized (this) {
					notification = new Boolean(false);
					this.notify();
				}
			}
		}

		/* (non-Javadoc)
		 * @see org.epics.ca.client.ChannelRequester#channelStateChange(org.epics.ca.client.Channel, org.epics.ca.client.Channel.ConnectionState)
		 */
		@Override
		public void channelStateChange(
				org.epics.ca.client.Channel c,
				ConnectionState connectionStatus) {
 			synchronized (this) {
				notification = new Boolean(connectionStatus == ConnectionState.CONNECTED);
				this.notify();
			}
		}

		@Override
		public String getRequesterName() {
			return this.getClass().getName();
		}

		@Override
		public void message(String message, MessageType messageType) {
			System.err.println("[" + messageType + "] " + message);
		}
		
		public void waitAndCheck() {
			synchronized (this) {
				if (notification == null)
				{
					try {
						//final long t1 = System.currentTimeMillis();
						this.wait(getTimeoutMs());
						//final long t2 = System.currentTimeMillis();
					} catch (InterruptedException e) {
						e.printStackTrace();
						// noop
					}
				}

				assertNotNull("channel connect timeout", notification);
				assertTrue("channel not connected", notification.booleanValue());
			}
		}
    };

	private Channel syncCreateChannel(String name) throws Throwable
	{
		ConnectionListener cl = new ConnectionListener();
	    Channel ch = getChannelProvider().createChannel(name, cl, CAConstants.CA_DEFAULT_PRIORITY);
	    registerChannelForDestruction(ch);
	    cl.waitAndCheck();
		return ch;
	}

    private static PVDataCreate pvDataCreate = PVDataFactory.getPVDataCreate();

    private class ChannelGetRequesterImpl implements ChannelGetRequester
	{
		ChannelGet channelGet;
		BitSet bitSet;
		PVStructure pvStructure;

		private Boolean connected = null;
		private Boolean success = null;
		
		@Override
		public void channelGetConnect(Status status, ChannelGet channelGet, PVStructure pvStructure, BitSet bitSet) {
			synchronized (this) {
				this.channelGet = channelGet;
				this.pvStructure = pvStructure;
				this.bitSet = bitSet;

				connected = new Boolean(status.isOK());
				this.notify();
			}
		}
		
		public void waitAndCheckConnect()
		{
			waitAndCheckConnect(true);
		}
		
		public void waitAndCheckConnect(boolean expectedSuccess)
		{
			synchronized (this) {
				if (connected == null)
				{
					try {
						this.wait(getTimeoutMs());
					} catch (InterruptedException e) {
						// noop
					}
				}
				
				assertNotNull("channel get connect timeout", connected);
				if (expectedSuccess) {
					assertTrue("channel get failed to connect", connected.booleanValue());
					assertNotNull(pvStructure);
					assertNotNull(bitSet);
				}
				else {
					assertFalse("channel get has not failed to connect", connected.booleanValue());
				}
			}
		}

		public void syncGet(boolean lastRequest)
		{
			syncGet(lastRequest, true);
		}
		
		public void syncGet(boolean lastRequest, boolean expectedSuccess)
		{
			synchronized (this) {
				if (connected == null)
					assertNotNull("channel get not connected", connected);
					
				success = null;
				channelGet.get(lastRequest);
				
				try {
					if (success == null)
						this.wait(getTimeoutMs());
				} catch (InterruptedException e) {
					// noop
				}
				
				assertNotNull("channel get timeout", success);
				if (expectedSuccess)
					assertTrue("channel get failed", success.booleanValue());
				else
					assertFalse("channel get has not failed", success.booleanValue());
			}
		}
		
		@Override
		public void getDone(Status success) {
			synchronized (this) {
				this.success = new Boolean(success.isOK());
				this.notify();
			}
		}

		@Override
		public String getRequesterName() {
			return this.getClass().getName();
		}

		@Override
		public void message(String message, MessageType messageType) {
			System.err.println("[" + messageType + "] " + message);
		}
		
	};

	private class ChannelPutRequesterImpl implements ChannelPutRequester
	{
		ChannelPut channelPut;
		PVStructure pvStructure;
		BitSet bitSet;

		private Boolean connected = null;
		private Boolean success = null;

		@Override
		public void channelPutConnect(Status status, ChannelPut channelPut, PVStructure pvStructure, BitSet bitSet) {
			synchronized (this) {
				this.channelPut = channelPut;
				this.pvStructure = pvStructure;
				this.bitSet = bitSet;

				connected = new Boolean(status.isOK());
				this.notify();
			}
		}

		public void waitAndCheckConnect()
		{
			waitAndCheckConnect(true);
		}

		public void waitAndCheckConnect(boolean expectedSuccess)
		{
			synchronized (this) {
				if (connected == null)
				{
					try {
						this.wait(getTimeoutMs());
					} catch (InterruptedException e) {
						// noop
					}
				}
				
				assertNotNull("channel put connect timeout", connected);
				if (expectedSuccess) {
					assertTrue("channel put failed to connect", connected.booleanValue());
					assertNotNull(pvStructure);
					assertNotNull(bitSet);
				}
				else {
					assertFalse("channel put has not failed to connect", connected.booleanValue());
				}
			}
		}

		public void syncGet()
		{
			synchronized (this) {
				if (connected == null)
					assertNotNull("channel put not connected", connected);
					
				success = null;
				channelPut.get();
				
				try {
					if (success == null)
						this.wait(getTimeoutMs());
				} catch (InterruptedException e) {
					// noop
				}
				
				assertNotNull("channel get timeout", success);
				assertTrue("channel get failed", success.booleanValue());
			}
		}
		
		@Override
		public void getDone(Status success) {
			synchronized (this) {
				this.success = new Boolean(success.isOK());
				this.notify();
			}
		}
		
		public void syncPut(boolean lastRequest)
		{
			syncPut(lastRequest, true);
		}
		
		public void syncPut(boolean lastRequest, boolean expectedSuccess)
		{
			synchronized (this) {
				if (connected == null)
					assertNotNull("channel put not connected", connected);
					
				success = null;
				channelPut.put(lastRequest);
				
				try {
					if (success == null)
						this.wait(getTimeoutMs());
				} catch (InterruptedException e) {
					// noop
				}
				
				assertNotNull("channel put timeout", success);
				if (expectedSuccess)
					assertTrue("channel put failed", success.booleanValue());
				else
					assertFalse("channel put has not failed", success.booleanValue());
			}
		}
		
		@Override
		public void putDone(Status success) {
			synchronized (this) {
				this.success = new Boolean(success.isOK());
				this.notify();
			}
		}

		@Override
		public String getRequesterName() {
			return this.getClass().getName();
		}

		@Override
		public void message(String message, MessageType messageType) {
			System.err.println("[" + messageType + "] " + message);
		}
	};
	
	
	private class ChannelPutGetRequesterImpl implements ChannelPutGetRequester
	{
		ChannelPutGet channelPutGet;
		PVStructure pvPutStructure;
		PVStructure pvGetStructure;

		private Boolean connected = null;
		private Boolean success = null;

		/* (non-Javadoc)
		 * @see org.epics.ca.client.ChannelPutGetRequester#channelPutGetConnect(Status,org.epics.ca.client.ChannelPutGet, org.epics.pvData.pv.PVStructure, org.epics.pvData.pv.PVStructure)
		 */
		@Override
		public void channelPutGetConnect(
				Status status,
				ChannelPutGet channelPutGet,
				PVStructure pvPutStructure, PVStructure pvGetStructure) {
			synchronized (this)
			{
				this.channelPutGet = channelPutGet;
				this.pvPutStructure = pvPutStructure;
				this.pvGetStructure = pvGetStructure;

				connected = new Boolean(status.isOK());
				this.notify();
			}
		}


		public void waitAndCheckConnect()
		{
			waitAndCheckConnect(true);
		}
		
		public void waitAndCheckConnect(boolean expectedSuccess)
		{
			synchronized (this) {
				if (connected == null)
				{
					try {
						this.wait(getTimeoutMs());
					} catch (InterruptedException e) {
						// noop
					}
				}
				
				assertNotNull("channel put-get connect timeout", connected);
				if (expectedSuccess) {
					assertTrue("channel put-get failed to connect", connected.booleanValue());
					assertNotNull(pvPutStructure);
					assertNotNull(pvGetStructure);
				} else {
					assertFalse("channel put-get has not failed to connect", connected.booleanValue());
					assertNull(pvPutStructure);
					assertNull(pvGetStructure);
				}
			}
		}

		public void syncPutGet(boolean lastRequest)
		{
			syncPutGet(lastRequest, true);
		}
		
		public void syncPutGet(boolean lastRequest, boolean expectedSuccess)
		{
			synchronized (this) {
				if (connected == null)
					assertNotNull("channel put-get not connected", connected);
					
				success = null;
				channelPutGet.putGet(lastRequest);
				
				try {
					if (success == null)
						this.wait(getTimeoutMs());
				} catch (InterruptedException e) {
					// noop
				}
				
				assertNotNull("channel put-get timeout", success);
				if (expectedSuccess)
					assertTrue("channel put-get failed", success.booleanValue());
				else
					assertFalse("channel put-get has not failed", success.booleanValue());
			}
		}
	
		@Override
		public void putGetDone(Status success) {
			synchronized (this) {
				this.success = new Boolean(success.isOK());
				this.notify();
			}
		}

		public void syncGetGet()
		{
			syncGetGet(true);
		}
		
		public void syncGetGet(boolean expectedSuccess)
		{
			synchronized (this) {
				if (connected == null)
					assertNotNull("channel put-get not connected", connected);
					
				success = null;
				channelPutGet.getGet();
				
				try {
					if (success == null)
						this.wait(getTimeoutMs());
				} catch (InterruptedException e) {
					// noop
				}
				
				assertNotNull("channel get-get timeout", success);
				if (expectedSuccess)
					assertTrue("channel get-get failed", success.booleanValue());
				else
					assertFalse("channel get-get has not failed", success.booleanValue());
			}
		}
	
		@Override
		public void getGetDone(Status success) {
			synchronized (this) {
				this.success = new Boolean(success.isOK());
				this.notify();
			}
		}

		public void syncGetPut()
		{
			syncGetPut(true);
		}
		public void syncGetPut(boolean expectedSuccess)
		{
			synchronized (this) {
				if (connected == null)
					assertNotNull("channel put-get not connected", connected);
					
				success = null;
				channelPutGet.getPut();
				
				try {
					if (success == null)
						this.wait(getTimeoutMs());
				} catch (InterruptedException e) {
					// noop
				}
				
				assertNotNull("channel get-put timeout", success);
				if (expectedSuccess)
					assertTrue("channel get-put failed", success.booleanValue());
				else
					assertFalse("channel get-put has not failed", success.booleanValue());
			}
		}

		@Override
		public void getPutDone(Status success) {
			synchronized (this) {
				this.success = new Boolean(success.isOK());
				this.notify();
			}
		}

		@Override
		public String getRequesterName() {
			return this.getClass().getName();
		}

		@Override
		public void message(String message, MessageType messageType) {
			System.err.println("[" + messageType + "] " + message);
		}
		
	};
	
	ChannelProcessRequester channelProcessRequester = new ChannelProcessRequester() {
		
		volatile ChannelProcess channelProcess;
		
		@Override
		public void processDone(Status success) {
			System.out.println("processDone: " + success);
			channelProcess.process(true);
		}
		
		@Override
		public void channelProcessConnect(Status status, ChannelProcess channelProcess) {
			System.out.println("channelProcessConnect done");
			this.channelProcess = channelProcess;
			channelProcess.process(false);
		}
		
		@Override
		public String getRequesterName() {
			return this.getClass().getName();
		}

		@Override
		public void message(String message, MessageType messageType) {
			System.err.println("[" + messageType + "] " + message);
		}
		
	};

	private class GetFieldRequesterImpl implements GetFieldRequester {

		Field field;
		
		private Boolean success;
		
		@Override
		public void getDone(Status status, Field field) {
			synchronized (this) {
				this.field = field;
				this.success = new Boolean(status.isOK());
				this.notify();
			}
		}

		public void syncGetField(Channel ch, String subField)
		{
			syncGetField(ch, subField, true);
		}
		
		public void syncGetField(Channel ch, String subField, boolean expectedSucces)
		{
			synchronized (this) {
					
				success = null;
				ch.getField(this, subField);
				
				try {
					if (success == null)
						this.wait(getTimeoutMs());
				} catch (InterruptedException e) {
					// noop
				}
				
				assertNotNull("channel getField timeout", success);
				if (expectedSucces)
					assertTrue("channel getField failed", success.booleanValue());
				else
					assertFalse("channel getField has not failed", success.booleanValue());
			}
		}

		@Override
		public String getRequesterName() {
			return this.getClass().getName();
		}

		@Override
		public void message(String message, MessageType messageType) {
			System.err.println("[" + messageType + "] " + message);
		}
	};

	private class ChannelProcessRequesterImpl implements ChannelProcessRequester {
		
		ChannelProcess channelProcess;

		private Boolean success;
		private Boolean connected;
		
		@Override
		public void processDone(Status success) {
			synchronized (this) {
				this.success = new Boolean(success.isOK());
				this.notify();
			}
		}
		
		@Override
		public void channelProcessConnect(Status status, ChannelProcess channelProcess) {
			synchronized (this) {
				this.channelProcess = channelProcess;

				connected = new Boolean(status.isOK());
				this.notify();
			}
		}

		public void waitAndCheckConnect()
		{
			waitAndCheckConnect(true);
		}
		
		public void waitAndCheckConnect(boolean expectedSuccess)
		{
			synchronized (this) {
				if (connected == null)
				{
					try {
						this.wait(getTimeoutMs());
					} catch (InterruptedException e) {
						// noop
					}
				}
				
				assertNotNull("channel process connect timeout", connected);
				if (expectedSuccess)
					assertTrue("channel process failed to connect", connected.booleanValue());
				else
					assertFalse("channel process has not failed to connect", connected.booleanValue());
			}
		}
		
		public void syncProcess(boolean lastRequest)
		{
			syncProcess(lastRequest, true);
		}
		
		public void syncProcess(boolean lastRequest, boolean expectedSuccess)
		{
			synchronized (this) {
				if (connected == null)
					assertNotNull("channel process not connected", connected);
					
				success = null;
				channelProcess.process(lastRequest);
				
				try {
					if (success == null)
						this.wait(getTimeoutMs());
				} catch (InterruptedException e) {
					// noop
				}
				
				assertNotNull("channel process timeout", success);
				if (expectedSuccess)
					assertTrue("channel process failed", success.booleanValue());
				else
					assertFalse("channel process hass not failed", success.booleanValue());
			}
		}

		@Override
		public String getRequesterName() {
			return this.getClass().getName();
		}

		@Override
		public void message(String message, MessageType messageType) {
			System.err.println("[" + messageType + "] " + message);
		}
		
	};
	
	
	private class ChannelArrayRequesterImpl implements ChannelArrayRequester {

		ChannelArray channelArray;
		PVArray pvArray;
		
		private Boolean connected = null;
		private Boolean success = null;

		@Override
		public void channelArrayConnect(Status status, ChannelArray channelArray, PVArray pvArray) {
			synchronized (this)
			{
				this.channelArray = channelArray;
				this.pvArray = pvArray;

				connected = new Boolean(status.isOK());
				this.notify();
			}
		}

		public void waitAndCheckConnect()
		{
			waitAndCheckConnect(true);
		}

		public void waitAndCheckConnect(boolean expectedSuccess)
		{
			synchronized (this) {
				if (connected == null)
				{
					try {
						this.wait(getTimeoutMs());
					} catch (InterruptedException e) {
						// noop
					}
				}
				
				assertNotNull("channel array connect timeout", connected);
				if (expectedSuccess)
				{
					assertTrue("channel array failed to connect", connected.booleanValue());
					assertNotNull(pvArray);
				}
				else
				{
					assertFalse("channel array has not failed to connect", connected.booleanValue());
					assertNull(pvArray);
				}
			}
		}

		@Override
		public void getArrayDone(Status success) {
			synchronized (this) {
				this.success = new Boolean(success.isOK());
				this.notify();
			}
		}

		public void syncGet(boolean lastRequest, int offset, int count)
		{
			syncGet(lastRequest, offset, count, true);
		}
		
		public void syncGet(boolean lastRequest, int offset, int count, boolean expectedSuccess)
		{
			synchronized (this) {
				if (connected == null)
					assertNotNull("channel array not connected", connected);
					
				success = null;
				channelArray.getArray(lastRequest, offset, count);
				
				try {
					if (success == null)
						this.wait(getTimeoutMs());
				} catch (InterruptedException e) {
					// noop
				}
				
				assertNotNull("channel array get timeout", success);
				if (expectedSuccess)
					assertTrue("channel array get failed", success.booleanValue());
				else
					assertFalse("channel array get has not failed", success.booleanValue());
			}
		}

		@Override
		public void putArrayDone(Status success) {
			synchronized (this) {
				this.success = new Boolean(success.isOK());
				this.notify();
			}
		}

		public void syncPut(boolean lastRequest, int offset, int count)
		{
			syncPut(lastRequest, offset, count, true);
		}

		public void syncPut(boolean lastRequest, int offset, int count, boolean expectedSuccess)
		{
			synchronized (this) {
				if (connected == null)
					assertNotNull("channel array not connected", connected);
					
				success = null;
				channelArray.putArray(lastRequest, offset, count);
				
				try {
					if (success == null)
						this.wait(getTimeoutMs());
				} catch (InterruptedException e) {
					// noop
				}
				
				assertNotNull("channel array put timeout", success);
				if (expectedSuccess)
					assertTrue("channel array put failed", success.booleanValue());
				else
					assertFalse("channel array put has not failed", success.booleanValue());
			}
		}

		@Override
		public String getRequesterName() {
			return this.getClass().getName();
		}

		@Override
		public void message(String message, MessageType messageType) {
			System.err.println("[" + messageType + "] " + message);
		}
		
	};

	private class ChannelMonitorRequesterImpl implements MonitorRequester {
		
		PVStructure pvStructure;
		BitSet changeBitSet;
		BitSet overrunBitSet;
		
		AtomicInteger monitorCounter = new AtomicInteger();
			
		Monitor channelMonitor;
		
		private Boolean connected = null;

		@Override
		public void monitorConnect(Status status, Monitor channelMonitor, Structure structure) {
			synchronized (this)
			{
				this.channelMonitor = channelMonitor;

				connected = new Boolean(status.isOK());
				this.notify();
			}
		}

		public void waitAndCheckConnect()
		{
			synchronized (this) {
				if (connected == null)
				{
					try {
						this.wait(getTimeoutMs());
					} catch (InterruptedException e) {
						// noop
					}
				}
				
				assertNotNull("channel monitor connect timeout", connected);
				assertTrue("channel monitor failed to connect", connected.booleanValue());
			}
		}

		@Override
		public void unlisten(Monitor monitor) {
			// TODO Auto-generated method stub
			
		}
		
		/* (non-Javadoc)
		 * @see org.epics.pvData.monitor.MonitorRequester#monitorEvent(org.epics.pvData.monitor.Monitor)
		 */
		@Override
		public void monitorEvent(Monitor monitor) {
			synchronized (this) {
				MonitorElement monitorElement = monitor.poll();

				this.pvStructure = monitorElement.getPVStructure();
				this.changeBitSet = monitorElement.getChangedBitSet();
				this.overrunBitSet = monitorElement.getOverrunBitSet();

				assertNotNull(this.pvStructure);
				assertNotNull(this.changeBitSet);
				assertNotNull(this.overrunBitSet);
			
				monitorCounter.incrementAndGet();
				this.notify();

				monitor.release(monitorElement);
			}
		}

		@Override
		public String getRequesterName() {
			return this.getClass().getName();
		}

		@Override
		public void message(String message, MessageType messageType) {
			System.err.println("[" + messageType + "] " + message);
		}
	};

	public void testChannelGet() throws Throwable
	{
	    Channel ch = syncCreateChannel("valueOnly");
	    
	    channelGetTestParameters(ch);
	    
		channelGetTestNoProcess(ch, false);
		channelGetTestNoProcess(ch, true);
		
		ch.destroy();
		
	    ch = syncCreateChannel("simpleCounter");
		
		channelGetTestIntProcess(ch, false);
		channelGetTestIntProcess(ch, true);
		
		channelGetTestNoConnection(ch, true);
		channelGetTestNoConnection(ch, false);
		/*
		ch.destroy();
		channelGetTestNoConnection(ch, false);
		*/
	}
	
	private void channelGetTestParameters(Channel ch) throws Throwable
	{
    	PVStructure pvRequest = PVCopyFactory.createRequest("field(timeStamp,value)",ch);

    	try 
        {
        	ch.createChannelGet(null, pvRequest);
			fail("null ChannelGetRequester accepted");
		} catch (IllegalArgumentException th) {
			// OK
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}
		
		ChannelGetRequesterImpl channelGetRequester = new ChannelGetRequesterImpl();
		try 
        {
        	ch.createChannelGet(channelGetRequester, null);
			fail("null pvRequest accepted");
		} catch (IllegalArgumentException th) {
			// OK
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}
		
	}

	private void channelGetTestNoConnection(Channel ch, boolean disconnect) throws Throwable
	{
    	PVStructure pvRequest = PVCopyFactory.createRequest("field(timeStamp,value)",ch);

		ChannelGetRequesterImpl channelGetRequester = new ChannelGetRequesterImpl();
		ch.createChannelGet(channelGetRequester, pvRequest);
		channelGetRequester.waitAndCheckConnect(disconnect);
		if (disconnect) 
		{
			//ch.disconnect();
			ch.destroy();
			channelGetRequester.syncGet(false, false);
		}
	}
	
	private void channelGetTestNoProcess(Channel ch, boolean share) throws Throwable
	{
    	PVStructure pvRequest = PVCopyFactory.createRequest("field(timeStamp,value)",ch);

		ChannelGetRequesterImpl channelGetRequester = new ChannelGetRequesterImpl();
		ch.createChannelGet(channelGetRequester, pvRequest);
		channelGetRequester.waitAndCheckConnect();
		
		//assertEquals("get-test", channelGetRequester.pvStructure.getFullName());
		
		channelGetRequester.syncGet(false);
		// only first bit must be set
		assertEquals(1, channelGetRequester.bitSet.cardinality());
		assertTrue(channelGetRequester.bitSet.get(0));
		
		channelGetRequester.syncGet(false);
		// no changes
		assertEquals(0, channelGetRequester.bitSet.cardinality());

		channelGetRequester.syncGet(true);
		// no changes, again
		assertEquals(0, channelGetRequester.bitSet.cardinality());
		
		channelGetRequester.channelGet.destroy();
		// this must fail (callback with unsuccessful completion status)
		channelGetRequester.syncGet(true, false);
	}
	
	private void channelGetTestIntProcess(Channel ch, boolean share) throws Throwable
	{
    	PVStructure pvRequest = PVCopyFactory.createRequest("record[process=true]field(timeStamp,value)",ch);

		ChannelGetRequesterImpl channelGetRequester = new ChannelGetRequesterImpl();
		ch.createChannelGet(channelGetRequester, pvRequest);
		channelGetRequester.waitAndCheckConnect();
		
		//assertEquals("get-test", channelGetRequester.pvStructure.getFullName());
		PVInt value = channelGetRequester.pvStructure.getIntField("value");
		TimeStamp timestamp = TimeStampFactory.getTimeStamp(channelGetRequester.pvStructure.getStructureField("timeStamp"));
		
		channelGetRequester.syncGet(false);
		// only first bit must be set
		assertEquals(1, channelGetRequester.bitSet.cardinality());
		assertTrue(channelGetRequester.bitSet.get(0));
		
		// multiple tests 
		final int TIMES = 3;
		for (int i = 1; i <= TIMES; i++)
		{
			int previousValue = value.get();
			long previousTimestampSec = timestamp.getSecondsPastEpoch();
			
			// 2 seconds to have different timestamps
			Thread.sleep(1000);
			
			channelGetRequester.syncGet(i == TIMES);
			// changes of value and timeStamp
			assertEquals((previousValue + 1)%11, value.get());
			assertTrue(timestamp.getSecondsPastEpoch() > previousTimestampSec);
		}

		channelGetRequester.channelGet.destroy();
		// this must fail (callback with unsuccessful completion status)
		channelGetRequester.syncGet(true, false);
	}
	
	public void testChannelPut() throws Throwable
	{
	    Channel ch = syncCreateChannel("valueOnly");
	
	    channelPutTestParameters(ch);
	    
		channelPutTestNoProcess(ch, false);
		channelPutTestNoProcess(ch, true);
		
		ch.destroy();
		
	    ch = syncCreateChannel("simpleCounter");
		
		channelPutTestIntProcess(ch, false);
		channelPutTestIntProcess(ch, true);

		channelPutTestNoConnection(ch, true);
		channelPutTestNoConnection(ch, false);
		/*
		ch.destroy();
		channelPutTestNoConnection(ch, false);
		*/
	}
	
	private void channelPutTestParameters(Channel ch) throws Throwable
	{
    	PVStructure pvRequest = PVCopyFactory.createRequest("field(timeStamp,value)",ch);

        try 
        {
        	ch.createChannelPut(null, pvRequest);
			fail("null ChannelPutRequester accepted");
		} catch (IllegalArgumentException th) {
			// OK
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}
		
		ChannelPutRequesterImpl channelPutRequester = new ChannelPutRequesterImpl();
		try 
        {
        	ch.createChannelPut(channelPutRequester, null);
			fail("null pvRequest accepted");
		} catch (IllegalArgumentException th) {
			// OK
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}
		
	}

	private void channelPutTestNoConnection(Channel ch, boolean disconnect) throws Throwable
	{
    	PVStructure pvRequest = PVCopyFactory.createRequest("field(timeStamp,value)",ch);

		ChannelPutRequesterImpl channelPutRequester = new ChannelPutRequesterImpl();
		ch.createChannelPut(channelPutRequester, pvRequest);
		channelPutRequester.waitAndCheckConnect(disconnect);
		if (disconnect) 
		{
			//ch.disconnect();
			ch.destroy();
			channelPutRequester.syncPut(false, false);
		}
	}
	
	private void channelPutTestNoProcess(Channel ch, boolean share) throws Throwable
	{
    	PVStructure pvRequest = PVCopyFactory.createRequest("field(value)",ch);

		ChannelPutRequesterImpl channelPutRequester = new ChannelPutRequesterImpl();
		ch.createChannelPut(channelPutRequester, pvRequest);
		channelPutRequester.waitAndCheckConnect();
		
		//assertEquals("put-test", channelPutRequester.pvStructure.getFullName());

		// set and get test
		PVDouble value = channelPutRequester.pvStructure.getDoubleField("value");
		assertNotNull(value);
		final double INIT_VAL = 123.0;
		value.put(INIT_VAL);
		channelPutRequester.bitSet.set(value.getFieldOffset());
		
		channelPutRequester.syncPut(false);
		// TODO should put bitSet be reset here
		//assertEquals(0, channelPutRequester.bitSet.cardinality());
		channelPutRequester.syncGet();
		assertEquals(INIT_VAL, value.get());


		// value should not change since bitSet is not set
		// unless it is shared and local
		value.put(INIT_VAL+2);
		channelPutRequester.bitSet.clear();
		channelPutRequester.syncPut(false);
		channelPutRequester.syncGet();
		if (share && isLocal())
			assertEquals(INIT_VAL+2, value.get());
		else
			assertEquals(INIT_VAL, value.get());
			
		// now should change
		value.put(INIT_VAL+1);
		channelPutRequester.bitSet.set(value.getFieldOffset());
		channelPutRequester.syncPut(false);
		channelPutRequester.syncGet();
		assertEquals(INIT_VAL+1, value.get());

		// destroy
		channelPutRequester.syncPut(true);

		channelPutRequester.channelPut.destroy();
		// this must fail (callback with unsuccessful completion status)
		channelPutRequester.syncPut(true, false);
	}

	private void channelPutTestIntProcess(Channel ch, boolean share) throws Throwable
	{
    	PVStructure pvRequest = PVCopyFactory.createRequest("record[process=true]field(value)",ch);

		ChannelPutRequesterImpl channelPutRequester = new ChannelPutRequesterImpl();
		ch.createChannelPut(channelPutRequester, pvRequest);
		channelPutRequester.waitAndCheckConnect();
		
		//assertEquals("put-test", channelPutRequester.pvStructure.getFullName());

		// set and get test
		PVInt value = channelPutRequester.pvStructure.getIntField("value");
		assertNotNull(value);
		final int INIT_VAL = 3;
		value.put(INIT_VAL);
		channelPutRequester.bitSet.set(value.getFieldOffset());
		
		channelPutRequester.syncPut(false);
		// TODO should put bitSet be reset here
		//assertEquals(0, channelPutRequester.bitSet.cardinality());
		channelPutRequester.syncGet();
		assertEquals(INIT_VAL+1, value.get());	// +1 due to process


		// value should change only due to process
		value.put(INIT_VAL+3);
		channelPutRequester.bitSet.clear();
		channelPutRequester.syncPut(false);
		channelPutRequester.syncGet();
		if (share && isLocal())
			assertEquals(INIT_VAL+4, value.get());
		else
			assertEquals(INIT_VAL+2, value.get());
		
		// destroy
		channelPutRequester.syncPut(true);

		channelPutRequester.channelPut.destroy();
		// this must fail (callback with unsuccessful completion status)
		channelPutRequester.syncPut(true, false);
	}

	public void testChannelGetField() throws Throwable
	{
	    Channel ch = syncCreateChannel("simpleCounter");
	
		GetFieldRequesterImpl channelGetField = new GetFieldRequesterImpl();
		
		// get all
		channelGetField.syncGetField(ch, null);
		assertNotNull(channelGetField.field);
		assertEquals(Type.structure, channelGetField.field.getType());
		// TODO there is no name
		// assertEquals(ch.getChannelName(), channelGetField.field.getFieldName());
		
		// value only
		channelGetField.syncGetField(ch, "value");
		assertNotNull(channelGetField.field);
		assertEquals(Type.scalar, channelGetField.field.getType());
		assertEquals("value", channelGetField.field.getFieldName());

		// non-existant
		channelGetField.syncGetField(ch, "invalid", false);
		assertNull(channelGetField.field);
		
		/*
		ch.disconnect();
		
		channelGetField.syncGetField(ch, "value", false);
		*/
		
		ch.destroy();
		
		channelGetField.syncGetField(ch, "value", false);
	}
	
	public void testChannelProcess() throws Throwable
	{
	    Channel ch = syncCreateChannel("simpleCounter");

		// create get to check processing
    	PVStructure pvRequest = PVCopyFactory.createRequest("field(value)",ch);

		ChannelGetRequesterImpl channelGetRequester = new ChannelGetRequesterImpl();
		ch.createChannelGet(channelGetRequester, pvRequest);
		channelGetRequester.waitAndCheckConnect();
		
		// get initial state
		channelGetRequester.syncGet(false);

		// null requester test
        try 
        {
        	ch.createChannelProcess(null, null);
			fail("null ChannelProcessRequester accepted");
		} catch (IllegalArgumentException th) {
			// OK
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}
		
		// create process
		ChannelProcessRequesterImpl channelProcessRequester = new ChannelProcessRequesterImpl();
		ch.createChannelProcess(channelProcessRequester, null);
		channelProcessRequester.waitAndCheckConnect();
		
		// there should be no changes
		channelGetRequester.syncGet(false);
		assertEquals(0, channelGetRequester.bitSet.cardinality());
		
		channelProcessRequester.syncProcess(false);

		// there should be a change
		channelGetRequester.syncGet(false);
		assertEquals(1, channelGetRequester.bitSet.cardinality());
		/*
		// now let's try to create another processor :)
		ChannelProcessRequesterImpl channelProcessRequester2 = new ChannelProcessRequesterImpl();
		ch.createChannelProcess(channelProcessRequester2, null);
		channelProcessRequester2.waitAndCheckConnect();
		
		// and process
		channelProcessRequester2.syncProcess(false);

		// there should be a change
		channelGetRequester.syncGet(false);
		assertEquals(1, channelGetRequester.bitSet.cardinality());
		
		// TODO since there is no good error handling I do not know that creating of second process failed !!!
		// however it shoudn't, right!!!
		
		// check if process works with destroy option
		channelProcessRequester.syncProcess(true);

		// there should be a change
		channelGetRequester.syncGet(false);
		assertEquals(1, channelGetRequester.bitSet.cardinality());
		 */
		channelProcessRequester.channelProcess.destroy();
		// this must fail (callback with unsuccessful completion status)
		channelProcessRequester.syncProcess(true, false);
		
		channelProcessTestNoConnection(ch, true);
		channelProcessTestNoConnection(ch, false);
		/*
		ch.destroy();
		channelProcessTestNoConnection(ch, false);
		*/
	}

	private void channelProcessTestNoConnection(Channel ch, boolean disconnect) throws Throwable
	{
		ChannelProcessRequesterImpl channelProcessRequester = new ChannelProcessRequesterImpl();
		ch.createChannelProcess(channelProcessRequester, null);
		channelProcessRequester.waitAndCheckConnect(disconnect);
		if (disconnect) 
		{
			//ch.disconnect();
			ch.destroy();
			channelProcessRequester.syncProcess(false, false);
		}
	}
	/*
	public void testChannelTwoGetProcess() throws Throwable
	{
		Channel ch = syncCreateChannel("simpleCounter");

		// create gets to check processing
    	PVStructure pvRequest = PVCopyFactory.createRequest("record[process=true]field(value)",ch);

		ChannelGetRequesterImpl channelGetRequester = new ChannelGetRequesterImpl();
		ch.createChannelGet(channelGetRequester, pvRequest, "get-process-test", true, true, null);
		channelGetRequester.waitAndCheckConnect();
		
		// get initial state
		channelGetRequester.syncGet(false);
		
		// there should be a change
		channelGetRequester.syncGet(false);
		assertEquals(1, channelGetRequester.bitSet.cardinality());

		// another get
		ChannelGetRequesterImpl channelGetRequester2 = new ChannelGetRequesterImpl();
		ch.createChannelGet(channelGetRequester2, pvRequest, "get-process-test-2", true, true, null);
		channelGetRequester2.waitAndCheckConnect();
		
		// get initial state
		channelGetRequester2.syncGet(false);

		// there should be a change too
		channelGetRequester2.syncGet(false);
		assertEquals(1, channelGetRequester2.bitSet.cardinality());

		// TODO since there is no good error handling I do not know that creating of second process failed !!!
		// however it shoudn't, right!!!
	}
	*/
	
	public void testChannelPutGet() throws Throwable
	{
	    Channel ch = syncCreateChannel("valueOnly");

	    channelPutGetTestParameters(ch);
	
		channelPutGetTestNoProcess(ch, false);
		channelPutGetTestNoProcess(ch, true);
		
		ch.destroy();
		
	    ch = syncCreateChannel("simpleCounter");
		
		channelPutGetTestIntProcess(ch, false);
		channelPutGetTestIntProcess(ch, true);

		channelPutGetTestNoConnection(ch, true);
		channelPutGetTestNoConnection(ch, false);
		
		/*
		ch.destroy();
		channelPutGetTestNoConnection(ch, false);
		*/
	}
	
	private void channelPutGetTestParameters(Channel ch) throws Throwable
	{
    	PVStructure pvRequest = PVCopyFactory.createRequest("putField(value)getField(timeStamp,value)",ch);
		
        try 
        {
        	ch.createChannelPutGet(null, pvRequest);
			fail("null ChannelProcessRequester accepted");
		} catch (IllegalArgumentException th) {
			// OK
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}
		
		ChannelPutGetRequesterImpl channelPutGetRequester = new ChannelPutGetRequesterImpl();
		try 
        {
        	ch.createChannelPutGet(channelPutGetRequester, null);
			fail("null pvRequest accepted");
		} catch (IllegalArgumentException th) {
			// OK
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}
	}

	private void channelPutGetTestNoConnection(Channel ch, boolean disconnect) throws Throwable
	{
    	PVStructure pvRequest = PVCopyFactory.createRequest("putField(value)getField(timeStamp,value)",ch);

		ChannelPutGetRequesterImpl channelPutGetRequester = new ChannelPutGetRequesterImpl();
		ch.createChannelPutGet(channelPutGetRequester, pvRequest);
		channelPutGetRequester.waitAndCheckConnect(disconnect);
		if (disconnect) 
		{
			//ch.disconnect();
			ch.destroy();
			channelPutGetRequester.syncPutGet(false, false);
			channelPutGetRequester.syncGetGet(false);
			channelPutGetRequester.syncGetPut(false);
		}
	}

	private void channelPutGetTestNoProcess(Channel ch, boolean share) throws Throwable
	{
		// TODO share
    	PVStructure pvRequest = PVCopyFactory.createRequest("putField(value)getField(timeStamp,value)",ch);

        ChannelPutGetRequesterImpl channelPutGetRequester = new ChannelPutGetRequesterImpl();
		ch.createChannelPutGet(channelPutGetRequester, pvRequest);
		channelPutGetRequester.waitAndCheckConnect();
		
		//assertEquals("put-test", channelPutGetRequester.pvPutStructure.getFullName());
		//assertEquals("get-test", channelPutGetRequester.pvGetStructure.getFullName());

		// set and get test
		PVDouble putValue = channelPutGetRequester.pvPutStructure.getDoubleField("value");
		assertNotNull(putValue);
		final double INIT_VAL = 321.0;
		putValue.put(INIT_VAL);
		
		PVDouble getValue = channelPutGetRequester.pvGetStructure.getDoubleField("value");
		assertNotNull(getValue);

		
		channelPutGetRequester.syncPutGet(false);
		assertEquals(INIT_VAL, getValue.get());

		// again
		putValue.put(INIT_VAL+1);
		channelPutGetRequester.syncPutGet(false);
		assertEquals(INIT_VAL+1, getValue.get());

		// test get-put
		channelPutGetRequester.syncGetPut();
		// TODO
		
		// test get-get
		channelPutGetRequester.syncGetGet();
		// TODO
		
		// destroy
		channelPutGetRequester.syncPutGet(true);

		channelPutGetRequester.channelPutGet.destroy();
		// this must fail (callback with unsuccessful completion status)
		channelPutGetRequester.syncPutGet(true, false);
	}

	private void channelPutGetTestIntProcess(Channel ch, boolean share) throws Throwable
	{
		// TODO share
    	PVStructure pvRequest = PVCopyFactory.createRequest("record[process=true]putField(value)getField(timeStamp,value)",ch);

        ChannelPutGetRequesterImpl channelPutGetRequester = new ChannelPutGetRequesterImpl();
		ch.createChannelPutGet(channelPutGetRequester, pvRequest);
		channelPutGetRequester.waitAndCheckConnect();
		
		//assertEquals("put-test", channelPutGetRequester.pvPutStructure.getFullName());
		//assertEquals("get-test", channelPutGetRequester.pvGetStructure.getFullName());

		// set and get test
		PVInt putValue = channelPutGetRequester.pvPutStructure.getIntField("value");
		assertNotNull(putValue);
		final int INIT_VAL = 3;
		putValue.put(INIT_VAL);
		
		PVInt getValue = channelPutGetRequester.pvGetStructure.getIntField("value");
		assertNotNull(getValue);
		TimeStamp timestamp = TimeStampFactory.getTimeStamp(channelPutGetRequester.pvGetStructure.getStructureField("timeStamp"));
		assertNotNull(timestamp);

		// get all
		channelPutGetRequester.syncGetGet();
		
		// multiple tests 
		final int TIMES = 3;
		for (int i = 1; i <= TIMES; i++)
		{
			int previousValue = getValue.get();
			long previousTimestampSec = timestamp.getSecondsPastEpoch();
			
			putValue.put((previousValue + 1)%11);
			
			// 2 seconds to have different timestamps
			Thread.sleep(1500);
			
			channelPutGetRequester.syncPutGet(i == TIMES);
			// changes of value and timeStamp; something is not right here...
			assertEquals((previousValue + 1 + 1)%11, getValue.get());	// +1 (new value) +1 (process)
			assertTrue(timestamp.getSecondsPastEpoch() > previousTimestampSec);
		}

		channelPutGetRequester.channelPutGet.destroy();
		// this must fail (callback with unsuccessful completion status)
		channelPutGetRequester.syncPutGet(true, false);
	}
	
	public void testChannelArray() throws Throwable
	{
	    Channel ch = syncCreateChannel("simpleCounter");
	    
		// null requester test
        try 
        {
        	ch.createChannelArray(null, null);
			fail("null ChannelArrayRequester accepted");
		} catch (IllegalArgumentException th) {
			// OK
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}

//    	PVStructure pvRequest = PVCopyFactory.createRequest("field(value)",ch);
    	PVStructure pvRequest = pvDataCreate.createPVStructure(null, "", new Field[0]);
    	PVString pvFieldName = (PVString)pvDataCreate.createPVScalar(pvRequest, "field", ScalarType.pvString);
    	pvFieldName.put("alarm.severity.choices");
    	pvRequest.appendPVField(pvFieldName);

		ChannelArrayRequesterImpl channelArrayRequester = new ChannelArrayRequesterImpl();
	    ch.createChannelArray(channelArrayRequester, pvRequest);
	    channelArrayRequester.waitAndCheckConnect();
	    
	    // test get
	    PVStringArray array = (PVStringArray)channelArrayRequester.pvArray;
	    StringArrayData data = new StringArrayData();
	    channelArrayRequester.syncGet(true, 1, 2);
	    int count = array.get(0, 100, data);
	    assertEquals(2, count);
	    assertTrue(Arrays.equals(new String[] { "minor", "major" }, data.data));
	 
	    ch.destroy();

		channelArrayRequester.channelArray.destroy();
		// this must fail (callback with unsuccessful completion status)
		channelArrayRequester.syncGet(true, 1, 2, false);
	    
    	pvFieldName.put("value");
	    ch = syncCreateChannel("arrayValueOnly");
	    channelArrayRequester = new ChannelArrayRequesterImpl();
	    ch.createChannelArray(channelArrayRequester, pvRequest);
	    channelArrayRequester.waitAndCheckConnect();
	    
	    // test put
	    PVDoubleArray doubleArray = (PVDoubleArray)channelArrayRequester.pvArray;
	    final double[] ARRAY_VALUE = new double[] { 1.1, 2.2, 3.3, 4.4, 5.5 }; 
	    doubleArray.put(0, ARRAY_VALUE.length, ARRAY_VALUE, 0);
	    channelArrayRequester.syncPut(false, 0, -1);
	    channelArrayRequester.syncGet(false, 0, ARRAY_VALUE.length /*-1*/); // this allows multiple runs on the same JavaIOC
	    DoubleArrayData doubleData = new DoubleArrayData();
	    count = doubleArray.get(0, 100, doubleData);
	    assertEquals(ARRAY_VALUE.length, count);
	    for (int i = 0; i < count; i++)
	    	assertEquals(ARRAY_VALUE[i], doubleData.data[i]);
	    
	    channelArrayRequester.syncPut(false, 4, -1);
	    channelArrayRequester.syncGet(false, 3, 3);
	    count = doubleArray.get(0, 3, doubleData);
	    assertEquals(3, count);
	    final double[] EXPECTED_VAL = new double[] { 4.4, 1.1, 2.2 };
	    for (int i = 0; i < count; i++)
	    	assertEquals(EXPECTED_VAL[i], doubleData.data[i]);

		channelArrayTestNoConnection(ch, true);
		channelArrayTestNoConnection(ch, false);
		/*
		ch.destroy();
		channelArrayTestNoConnection(ch, false);
		*/
	}

	private void channelArrayTestNoConnection(Channel ch, boolean disconnect) throws Throwable
	{
		ChannelArrayRequesterImpl channelArrayRequester = new ChannelArrayRequesterImpl();
    	PVStructure pvRequest = pvDataCreate.createPVStructure(null, "", new Field[0]);
    	PVString pvFieldName = (PVString)pvDataCreate.createPVScalar(pvRequest, "field", ScalarType.pvString);
    	pvFieldName.put("value");
    	pvRequest.appendPVField(pvFieldName);
		ch.createChannelArray(channelArrayRequester, pvRequest);
		channelArrayRequester.waitAndCheckConnect(disconnect);
		if (disconnect) 
		{
			//ch.disconnect();
			ch.destroy();
			channelArrayRequester.syncGet(false, 1, 2, false);
			channelArrayRequester.syncPut(false, 1, 2, false);
		}
	}

	private static final Convert convert = ConvertFactory.getConvert();

	public void testChannelMonitors() throws Throwable
	{
        Channel ch = syncCreateChannel("counter");

    	PVStructure pvRequest = PVCopyFactory.createRequest("record[queueSize=3]field(timeStamp)",ch);

        // null requester test
        try 
        {
        	ch.createMonitor(null, pvRequest);
			fail("null MonitorRequester accepted");
		} catch (IllegalArgumentException th) {
			// OK
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}
		
        // null pvRequest test
	    ChannelMonitorRequesterImpl channelMonitorRequester = new ChannelMonitorRequesterImpl();
        try 
        {
        	ch.createMonitor(channelMonitorRequester, null);
			fail("null pvRequest accepted");
		} catch (IllegalArgumentException th) {
			// OK
		} catch (Throwable th) {
			fail("other than IllegalArgumentException exception was thrown");
		}

		ch.destroy();

		channelMonitorTest(10);
		channelMonitorTest(2);
		channelMonitorTest(1);
//		channelMonitorTest(0);
	}

	public void channelMonitorTest(int queueSize) throws Throwable
	{
        Channel ch = syncCreateChannel("counter");
		
    	PVStructure pvRequest = PVCopyFactory.createRequest("record[queueSize=" + queueSize + "]field(timeStamp,value,alarm.severity.choices)",ch);
    	// TODO algorithm

    	ChannelMonitorRequesterImpl channelMonitorRequester = new ChannelMonitorRequesterImpl();
	    ch.createMonitor(channelMonitorRequester, pvRequest);
	    channelMonitorRequester.waitAndCheckConnect();

	    // not start, no monitors
	    assertEquals(0, channelMonitorRequester.monitorCounter.get());
	    
	    synchronized (channelMonitorRequester) {
		    channelMonitorRequester.channelMonitor.start();
		    
		    if (channelMonitorRequester.monitorCounter.get() == 0)
		    	channelMonitorRequester.wait(getTimeoutMs());
		    //assertEquals("monitor-test", channelMonitorRequester.pvStructure.getFullName());
		    assertEquals(1, channelMonitorRequester.monitorCounter.get());
		    assertEquals(1, channelMonitorRequester.changeBitSet.cardinality());
		    assertTrue(channelMonitorRequester.changeBitSet.get(0));

		    PVField valueField = channelMonitorRequester.pvStructure.getSubField("value");
		    PVField previousValue = pvDataCreate.createPVField(null, valueField.getField());
		    convert.copy(valueField, previousValue);
		    assertTrue(valueField.equals(previousValue));
		    
		    // all subsequent only timestamp and value
		    for (int i = 2; i < 5; i++) {
			    channelMonitorRequester.wait(getTimeoutMs());
			    //assertEquals("monitor-test", channelMonitorRequester.pvStructure.getFullName());
			    assertEquals(i, channelMonitorRequester.monitorCounter.get());
			    if (queueSize == 1)
			    {
				    assertEquals(1, channelMonitorRequester.changeBitSet.cardinality());
				    assertTrue(channelMonitorRequester.changeBitSet.get(0));
			    }
			    else
			    {
				    assertEquals(2, channelMonitorRequester.changeBitSet.cardinality());
				    assertTrue(channelMonitorRequester.changeBitSet.get(1));
				    assertTrue(channelMonitorRequester.changeBitSet.get(4));
			    }
			    
			    valueField = channelMonitorRequester.pvStructure.getSubField("value");
			    assertFalse(valueField.equals(previousValue));
			    convert.copy(valueField, previousValue);
		    }

		    channelMonitorRequester.channelMonitor.stop();
		    channelMonitorRequester.wait(500);
		    int mc = channelMonitorRequester.monitorCounter.get();
		    Thread.sleep(2000);
		    // no more monitors
		    assertEquals(mc, channelMonitorRequester.monitorCounter.get());
	    }
	}	
	
	/** ----------------- stress tests -------------- **/
	
    public void testStressConnectDisconnect() throws Throwable
    {
    	final int COUNT = 300;
    	for (int i = 1; i <= COUNT; i++)
    	{
    		Channel channel = syncCreateChannel("valueOnly");
    		channel.destroy();
    	}
    }
	
    public void testStressMonitorAndProcess() throws Throwable
    {
        Channel ch = syncCreateChannel("simpleCounter");
		
    	PVStructure pvRequest = PVCopyFactory.createRequest("record[queueSize=3]field(timeStamp,value,alarm.severity.choices)",ch);
    	// TODO algorithm onPut

    	ChannelMonitorRequesterImpl channelMonitorRequester = new ChannelMonitorRequesterImpl();
	    ch.createMonitor(channelMonitorRequester, pvRequest);
	    channelMonitorRequester.waitAndCheckConnect();

	    // not start, no monitors
	    assertEquals(0, channelMonitorRequester.monitorCounter.get());
	    
	    synchronized (channelMonitorRequester) {
		    channelMonitorRequester.channelMonitor.start();
		    
		    if (channelMonitorRequester.monitorCounter.get() == 0)
		    	channelMonitorRequester.wait(getTimeoutMs());
		    assertEquals(1, channelMonitorRequester.monitorCounter.get());
	    }

		// create process
		ChannelProcessRequesterImpl channelProcessRequester = new ChannelProcessRequesterImpl();
		ch.createChannelProcess(channelProcessRequester, null);
		channelProcessRequester.waitAndCheckConnect();

		final int COUNT = 1000;
		for (int i = 2; i < COUNT; i++)
		{
			channelProcessRequester.syncProcess(false);
			synchronized (channelMonitorRequester) {
			    if (channelMonitorRequester.monitorCounter.get() < i)
			    	channelMonitorRequester.wait(getTimeoutMs());
				assertEquals(i, channelMonitorRequester.monitorCounter.get());
			}
		}
	    
    }
	
	/** ----------------- ... and at last destroy() -------------- **/
	
	public void testDestroy() throws Throwable
	{
		final ChannelProvider provider = getChannelProvider();

		provider.destroy();

		//
		// multiple destroy test
		//
		// noop expected
		provider.destroy();
	}

}