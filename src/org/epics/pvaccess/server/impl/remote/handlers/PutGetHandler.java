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

package org.epics.pvaccess.server.impl.remote.handlers;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import org.epics.pvaccess.client.ChannelPutGet;
import org.epics.pvaccess.client.ChannelPutGetRequester;
import org.epics.pvaccess.impl.remote.QoS;
import org.epics.pvaccess.impl.remote.SerializationHelper;
import org.epics.pvaccess.impl.remote.Transport;
import org.epics.pvaccess.impl.remote.TransportSendControl;
import org.epics.pvaccess.impl.remote.TransportSender;
import org.epics.pvaccess.impl.remote.server.ChannelHostingTransport;
import org.epics.pvaccess.server.impl.remote.ServerChannelImpl;
import org.epics.pvaccess.server.impl.remote.ServerContextImpl;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.Status;
import org.epics.pvdata.pv.Status.StatusType;

/**
 * Put-get handler.
 * @author <a href="mailto:matej.sekoranjaATcosylab.com">Matej Sekoranja</a>
 * @version $Id$
 */
public class PutGetHandler extends AbstractServerResponseHandler {

	/**
	 * @param context
	 */
	public PutGetHandler(ServerContextImpl context) {
		super(context, "Put-get request");
	}

    
	private static class ChannelPutGetRequesterImpl extends BaseChannelRequester implements ChannelPutGetRequester, TransportSender {
		
		private volatile ChannelPutGet channelPutGet;
		private volatile PVStructure pvPutStructure;
		private volatile PVStructure pvGetStructure;
		private Status status;
		
		public ChannelPutGetRequesterImpl(ServerContextImpl context, ServerChannelImpl channel, int ioid, Transport transport,
				PVStructure pvRequest) {
			super(context, channel, ioid, transport);
			
			startRequest(QoS.INIT.getMaskValue());
			channel.registerRequest(ioid, this);
			
			try {
				channelPutGet = channel.getChannel().createChannelPutGet(this, pvRequest);
			} catch (Throwable th) {
				// simply cannot trust code above
				BaseChannelRequester.sendFailureMessage((byte)12, transport, ioid, (byte)QoS.INIT.getMaskValue(),
						statusCreate.createStatus(StatusType.FATAL, "Unexpected exception caught: " + th.getMessage(), th));
				destroy();
			}
		}

		/* (non-Javadoc)
		 * @see org.epics.pvaccess.client.ChannelPutGetRequester#channelPutGetConnect(org.epics.pvaccess.client.ChannelPutGet, org.epics.pvdata.pv.PVStructure, org.epics.pvdata.pv.PVStructure)
		 */
		@Override
		public void channelPutGetConnect(Status status, ChannelPutGet channelPutGet,
				PVStructure pvPutStructure, PVStructure pvGetStructure) {
			synchronized (this) {
				this.pvPutStructure = pvPutStructure;
				this.pvGetStructure = pvGetStructure;
				this.status = status;
				this.channelPutGet = channelPutGet;
			}
			transport.enqueueSendRequest(this);

			// self-destruction
			if (!status.isSuccess()) {
				destroy();
			}
		}

		/* (non-Javadoc)
		 * @see org.epics.pvaccess.client.ChannelPutGetRequester#getGetDone(Status)
		 */
		@Override
		public void getGetDone(Status status) {
			synchronized (this)
			{
				this.status = status;
			}
			transport.enqueueSendRequest(this);
		}

		/* (non-Javadoc)
		 * @see org.epics.pvaccess.client.ChannelPutGetRequester#getPutDone(Status)
		 */
		@Override
		public void getPutDone(Status status) {
			synchronized (this)
			{
				this.status = status;
			}
			transport.enqueueSendRequest(this);
		}

		/* (non-Javadoc)
		 * @see org.epics.pvaccess.client.ChannelPutGetRequester#putGetDone(Status)
		 */
		@Override
		public void putGetDone(Status status) {
			synchronized (this)
			{
				this.status = status;
			}
			transport.enqueueSendRequest(this);
		}

		/* (non-Javadoc)
		 * @see org.epics.pvdata.misc.Destroyable#destroy()
		 */
		@Override
		public void destroy() {
			channel.unregisterRequest(ioid);
			if (channelPutGet != null)
				channelPutGet.destroy();
		}

		/**
		 * @return the channelPutGet
		 */
		public ChannelPutGet getChannelPutGet() {
			return channelPutGet;
		}

		/**
		 * @return the pvPutStructure
		 */
		public PVStructure getPVPutStructure() {
			return pvPutStructure;
		}
		
		/* (non-Javadoc)
		 * @see org.epics.pvaccess.impl.remote.TransportSender#lock()
		 */
		@Override
		public void lock() {
			// TODO
		}

		/* (non-Javadoc)
		 * @see org.epics.pvaccess.impl.remote.TransportSender#unlock()
		 */
		@Override
		public void unlock() {
			// TODO
		}

		/* (non-Javadoc)
		 * @see org.epics.pvaccess.impl.remote.TransportSender#send(java.nio.ByteBuffer, org.epics.pvaccess.impl.remote.TransportSendControl)
		 */
		@Override
		public void send(ByteBuffer buffer, TransportSendControl control) {
			final int request = getPendingRequest();

			control.startMessage((byte)12, Integer.SIZE/Byte.SIZE + 1);
			buffer.putInt(ioid);
			buffer.put((byte)request);
			synchronized (this) {
				status.serialize(buffer, control);
			}

			if (status.isSuccess())
			{
				if (QoS.INIT.isSet(request))
				{
					control.cachedSerialize(pvPutStructure != null ? pvPutStructure.getField() : null, buffer);
					control.cachedSerialize(pvGetStructure != null ? pvGetStructure.getField() : null, buffer);
				}
				else if (QoS.GET.isSet(request))
				{
					pvGetStructure.serialize(buffer, control);
				}
				else if (QoS.GET_PUT.isSet(request))
				{
					pvPutStructure.serialize(buffer, control);
				}
				else
				{
					pvGetStructure.serialize(buffer, control);
				}
			}
			
			stopRequest();

			// lastRequest
			if (QoS.DESTROY.isSet(request))
				destroy();
		}
		
	};

	/* (non-Javadoc)
	 * @see org.epics.pvaccess.impl.remote.AbstractResponseHandler#handleResponse(java.net.InetSocketAddress, org.epics.pvaccess.core.Transport, byte, byte, int, java.nio.ByteBuffer)
	 */
	@Override
	public void handleResponse(InetSocketAddress responseFrom, final Transport transport, byte version, byte command, int payloadSize, ByteBuffer payloadBuffer) {
		super.handleResponse(responseFrom, transport, version, command, payloadSize, payloadBuffer);

		// NOTE: we do not explicitly check if transport is OK
		final ChannelHostingTransport casTransport = (ChannelHostingTransport)transport;

		transport.ensureData(2*Integer.SIZE/Byte.SIZE+1);
		final int sid = payloadBuffer.getInt();
		final int ioid = payloadBuffer.getInt();

		final byte qosCode = payloadBuffer.get();

		final ServerChannelImpl channel = (ServerChannelImpl)casTransport.getChannel(sid);
		if (channel == null) {
			BaseChannelRequester.sendFailureMessage((byte)12, transport, ioid, qosCode, BaseChannelRequester.badCIDStatus);
			return;
		}
		
		final boolean init = QoS.INIT.isSet(qosCode);
		if (init)
		{
			/*
			// check process access rights
			if (process && !AccessRights.PROCESS.isSet(channel.getAccessRights()))
			{
				putGetFailureResponse(transport, ioid, qosCode, BaseChannelRequester.noProcessACLStatus);
				return;
			}
			*/

			// pvRequest
		    final PVStructure pvRequest = SerializationHelper.deserializePVRequest(payloadBuffer, transport);
		    
			// create...
		    new ChannelPutGetRequesterImpl(context, channel, ioid, transport, pvRequest);
		}
		else
		{
			final boolean lastRequest = QoS.DESTROY.isSet(qosCode);
			final boolean getGet = QoS.GET.isSet(qosCode);
			final boolean getPut = QoS.GET_PUT.isSet(qosCode);
			
			ChannelPutGetRequesterImpl request = (ChannelPutGetRequesterImpl)channel.getRequest(ioid);
			if (request == null) {
				BaseChannelRequester.sendFailureMessage((byte)12, transport, ioid, qosCode, BaseChannelRequester.badIOIDStatus);
				return;
			}

			if (!request.startRequest(qosCode)) {
				BaseChannelRequester.sendFailureMessage((byte)12, transport, ioid, qosCode, BaseChannelRequester.otherRequestPendingStatus);
				return;
			}

			/*
			// check write access rights
			if (!AccessRights.WRITE.isSet(channel.getAccessRights()))
			{
				putGetFailureResponse(transport, ioid, qosCode, BaseChannelRequester.noWriteACLStatus);
				if (lastRequest)
					request.destroy();
				return;
			}
			 */
			
			/*
			// check read access rights
			if (!AccessRights.READ.isSet(channel.getAccessRights()))
			{
				putGetFailureResponse(transport, ioid, qosCode, BaseChannelRequester.noReadACLStatus);
				if (lastRequest)
					request.destroy();
				return;
			}
			*/

			if (getGet)
			{
				request.getChannelPutGet().getGet();
			}
			else if (getPut)
			{
				request.getChannelPutGet().getPut();
			}
			else
			{
				// deserialize put data
				request.getPVPutStructure().deserialize(payloadBuffer, transport);
				request.getChannelPutGet().putGet(lastRequest);
			}
		}
	}
}