package org.epics.pvaccess.client.pvds.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import org.epics.pvaccess.PVFactory;
import org.epics.pvaccess.client.pvds.Protocol;
import org.epics.pvaccess.client.pvds.Protocol.GUIDPrefix;
import org.epics.pvaccess.client.pvds.Protocol.ProtocolVersion;
import org.epics.pvaccess.client.pvds.Protocol.SequenceNumberSet;
import org.epics.pvaccess.client.pvds.Protocol.SubmessageHeader;
import org.epics.pvaccess.client.pvds.Protocol.VendorId;
import org.epics.pvaccess.util.InetAddressUtil;
import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.misc.BitSet;
import org.epics.pvdata.pv.ByteArrayData;
import org.epics.pvdata.pv.DeserializableControl;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.PVByteArray;
import org.epics.pvdata.pv.PVField;
import org.epics.pvdata.pv.PVScalarArray;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.PVStructureArray;
import org.epics.pvdata.pv.PVUnion;
import org.epics.pvdata.pv.PVUnionArray;
import org.epics.pvdata.pv.Scalar;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.SerializableControl;
import org.epics.pvdata.pv.StringArrayData;
import org.epics.pvdata.pv.Structure;
import org.epics.pvdata.pv.StructureArrayData;
import org.epics.pvdata.pv.UnionArrayData;

public class TestPVDS {

	static class NoopSerializableControl implements SerializableControl
	{

		@Override
		public void flushSerializeBuffer() {
			// noop
		}

		@Override
		public void ensureBuffer(int size) {
			// noop
		}

		@Override
		public void alignBuffer(int alignment) {
			// TODO
		}

		@Override
		public void cachedSerialize(Field field, ByteBuffer buffer) {
			field.serialize(buffer, this);
		}
		
	}
	
	public static SerializableControl NOOP_SERIALIZABLE_CONTROL = new NoopSerializableControl();

	static class NoopDeserializableControl implements DeserializableControl
	{

		@Override
		public void ensureData(int size) {
			// noop
		}

		@Override
		public void alignData(int alignment) {
			// TODO
		}

		@Override
		public Field cachedDeserialize(ByteBuffer buffer) {
			return PVFactory.getFieldCreate().deserialize(buffer, this);
		}
	}
	
	public static DeserializableControl NOOP_DESERIALIZABLE_CONTROL = new NoopDeserializableControl();
	
	
	static class MessageReceiver 
	{
		SocketAddress receivedFrom;
		
		short sourceVersion;
		short sourceVendorId;
		byte[] sourceGuidPrefix = new byte[12];
		byte submessageId;
		byte submessageFlags;
		int submessageSize;
		//byte[] destGuidPrefix = new byte[12];
		//unicastReplyLocatorList;
		//multicastReplyLocatorList;
		//boolean haveTimestamp;
		//Time_t timestamp;
		
		// TODO EntityId
		int readerId;
		int writerId;
		
		public void reset() 
		{
			// it does not makes sence to reset
			// receivedFrom
			// sourceVersion, sourceVendorId, sourceGuidPrefix
			// since they are reset by every message header 
		}
	}

	static class MessageReceiverStatistics {
		long[] submessageType = new long[255];
	    long messageToSmall;
		long nonRTPSMessage;
	    long versionMismatch;
	    long vendorMismatch;

	    long invalidSubmessageSize;
	    long submesssageAlignmentMismatch;
	    
	    long unknownSubmessage;
	    long validMessage;
	    
	    long invalidMessage;
	    
	    long lastSeqNo;
	    long missedSN;
	    long receivedSN;
	    long lostSN;
	    long recoveredSN;
	    long ignoredSN;
	    
	    public void reset()
	    {
	    	Arrays.fill(submessageType, 0);
			messageToSmall = 0;
			nonRTPSMessage = 0;
		    versionMismatch = 0;
		    vendorMismatch = 0;

		    invalidSubmessageSize = 0;
		    submesssageAlignmentMismatch = 0;
		    
		    unknownSubmessage = 0;
		    validMessage = 0;
		    
		    invalidMessage = 0;
		    
		    lastSeqNo = 0;
		    missedSN = 0;
		    receivedSN = 0;
		    lostSN = 0;
		    recoveredSN = 0;
		    ignoredSN = 0;
	    }
	    
		@Override
		public String toString() {
			return "MessageReceiverStatistics [messageToSmall="
					+ messageToSmall + ", nonRTPSMessage=" + nonRTPSMessage
					+ ", versionMismatch=" + versionMismatch
					+ ", vendorMismatch=" + vendorMismatch
					+ ", invalidSubmessageSize=" + invalidSubmessageSize
					+ ", submesssageAlignmentMismatch="
					+ submesssageAlignmentMismatch + ", unknownSubmessage="
					+ unknownSubmessage + ", validMessage=" + validMessage
					+ ", invalidMessage=" + invalidMessage + ", lastSeqNo="
					+ lastSeqNo + ", missedSN=" + missedSN + ", receivedSN="
					+ receivedSN + ", lostSN=" + lostSN + ", recoveredSN="
					+ recoveredSN + ", ignoredSN=" + ignoredSN + "]";
		}
	    
	};
	
	static Logger log = Logger.getGlobal();

	// TODO move to proper place
	final static GUIDPrefix guidPrefix = GUIDPrefix.generateGUIDPrefix();
	
	
	static class RTPSMessageEndPoint {

		
		protected final DatagramChannel discoveryMulticastChannel;
		protected final DatagramChannel discoveryUnicastChannel;

		protected final NetworkInterface nif;
        protected InetAddress discoveryMulticastGroup;
        protected int discoveryMulticastPort;
        
        // TODO remove
        public DatagramChannel getDiscoveryMulticastChannel()
        {
        	return discoveryMulticastChannel;
        }
        
        // TODO remove
        public DatagramChannel getDiscoveryUnicastChannel()
        {
        	return discoveryUnicastChannel;
        }

        public RTPSMessageEndPoint(String multicastNIF, int domainId) throws Throwable
		{
			if (domainId > Protocol.MAX_DOMAIN_ID)
				throw new IllegalArgumentException("domainId >= " + String.valueOf(Protocol.MAX_DOMAIN_ID));

			if (multicastNIF == null)
				nif = InetAddressUtil.getLoopbackNIF();
			else
				nif = NetworkInterface.getByName(multicastNIF);

			if (nif == null)
				throw new IOException("no network interface available");
			
			System.out.println("NIF: " + nif.getDisplayName());
			
			
			// TODO configure IPv4 multicast prefix
	        discoveryMulticastGroup =
	        	InetAddress.getByName("239.255." + String.valueOf(domainId) + ".1");
	        discoveryMulticastPort = Protocol.PB + domainId * Protocol.DG + Protocol.d0;
	        
	        discoveryMulticastChannel = DatagramChannel.open(StandardProtocolFamily.INET)
	        	.setOption(StandardSocketOptions.SO_REUSEADDR, true)
//	        	.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
	        	.bind(new InetSocketAddress(discoveryMulticastPort));
	        
	        discoveryMulticastChannel.join(discoveryMulticastGroup, nif);
//	        discoveryMulticastChannel.configureBlocking(false);
		    discoveryMulticastChannel.configureBlocking(true);

	        
	        discoveryUnicastChannel = DatagramChannel.open(StandardProtocolFamily.INET)
//	        	.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true)
	    		.setOption(StandardSocketOptions.SO_REUSEADDR, false);
	        
	        int participantId;
	        int unicastDiscoveryPort = 0;
	        for (participantId = 0; participantId < Protocol.MAX_PARTICIPANT_ID; participantId++)
	        {
	        	unicastDiscoveryPort = Protocol.PB + domainId * Protocol.DG + participantId * Protocol.PG + Protocol.d1;
	        	try {
	        		discoveryUnicastChannel.bind(new InetSocketAddress(unicastDiscoveryPort));
	        		break;
	        	} catch (Throwable th) {
	        		// noop
	        	}
	        }
	        
	        if (participantId > Protocol.MAX_PARTICIPANT_ID)
	        	throw new RuntimeException("maximum number of participants on this host reached");
	        	
		    //discoveryUnicastChannel.configureBlocking(false);
		    discoveryUnicastChannel.configureBlocking(true);
		    
		    System.out.println("pvDS started: domainId = " + domainId + ", participantId = " + participantId);
		    System.out.println("pvDS discovery multicast group: " + discoveryMulticastGroup + ":" + discoveryMulticastPort);
		    System.out.println("pvDS unicast port: " + unicastDiscoveryPort);
		    System.out.println("pvDS GUID prefix: " + Arrays.toString(guidPrefix.value));
		}
		
	    public void addMessageHeader(ByteBuffer buffer)
	    {
		    // MessageHeader
	    	buffer.putLong(Protocol.HEADER_NO_GUID);
		    buffer.put(guidPrefix.value);
	    }

	    public void addSubmessageHeader(ByteBuffer buffer, 
	    		byte submessageId, byte submessageFlags, int octetsToNextHeaderPos)
	    {
	    	buffer.put(submessageId);
		    // E = SubmessageHeader.flags & 0x01 (0 = big, 1 = little)
	    	buffer.put(submessageFlags);
	    	buffer.putShort((short)octetsToNextHeaderPos);
	    }
		
	}
	
	
	static class RTPSMessageReceiver extends RTPSMessageEndPoint
	{
		private final int readerId;
		
		private final MessageReceiver receiver = new MessageReceiver();
	    private final MessageReceiverStatistics stats = new MessageReceiverStatistics();

	    public MessageReceiverStatistics getStatistics()
	    {
	    	return stats;
	    }
	    
	    private final ArrayList<FragmentationBufferEntry> fragmentationBuffers;
	    private int fragmentationBuffersIndex = 0;
	    
	    private final Map<Long, FragmentationBufferEntry> fragmentTable = 
	    	new HashMap<Long, FragmentationBufferEntry>();
	    
	    private final Field field;

	    public RTPSMessageReceiver(String multicastNIF, int domainId, int readerId, Field field) throws Throwable {
			super(multicastNIF, domainId);
			
			this.readerId = readerId;
			this.field = field;
			
			// TODO configurable
			int fragmentationBuffersSize = 3;
			fragmentationBuffers = new ArrayList<FragmentationBufferEntry>(fragmentationBuffersSize);
			
			// TODO max data size, depends on a structure!!!
			final int MAX_PAYLOAD_SIZE = 64*1024*1024;
		    ByteBuffer buffer = ByteBuffer.allocate(fragmentationBuffersSize*MAX_PAYLOAD_SIZE);
			
		    int pos = 0;
		    for (int i = 0; i < fragmentationBuffersSize; i++)
		    {
		    	buffer.position(pos);
		    	pos += MAX_PAYLOAD_SIZE;
		    	buffer.limit(pos);

		    	fragmentationBuffers.add(new FragmentationBufferEntry(buffer.slice()));
		    }
		    
		    System.out.println("Receiver: fragmentation buffer size = " + fragmentationBuffersSize + " packets of " + MAX_PAYLOAD_SIZE + " bytes (max payload size)");

		}
	    
	    private int ackNackCounter = 0;

	    // send it:
	    //    - periodically when some packets are missing on reader side and are available ob writer side
	    //    - when heartbeat with (no new data available received; same lastSN, different count)
	    //      and there are some missing packets that are available
	    //    - every N messages (N = 100?)
	    //	  - immediately when heartbeat with final flag is received
	    // try to piggyback
	    public void addAckNackSubmessage(ByteBuffer buffer, SequenceNumberSet readerSNState)
	    {
	    	// big endian flag
	    	addSubmessageHeader(buffer, SubmessageHeader.RTPS_ACKNACK, (byte)0x00, 0x0000);
		    int octetsToNextHeaderPos = buffer.position() - 2;
		    
		    // readerId
		    buffer.putInt(readerId);		
		    // writerId
		    buffer.putInt(0);
		    
		    // SequenceNumberSet readerSNState
		    readerSNState.serialize(buffer);
		    
		    // count
		    buffer.putInt(ackNackCounter++);

		    // set message size
		    int octetsToNextHeader = buffer.position() - octetsToNextHeaderPos - 2;
		    buffer.putShort(octetsToNextHeaderPos, (short)(octetsToNextHeader & 0xFFFF));
	    }

	    // TODO tmp
	    private int ackCounter = 0;
	    // try to piggyback
	    public void addAckSubmessage(ByteBuffer buffer, long ackSeqNo)
	    {
	    	// big endian flag
	    	addSubmessageHeader(buffer, SubmessageHeader.PVDS_ACK, (byte)0x00, 20);
		    
		    // readerId
		    buffer.putInt(readerId);		
		    // writerId
		    buffer.putInt(0);
		    
		    // ackSeqNo
		    buffer.putLong(ackSeqNo);
		    
		    // count
		    buffer.putInt(ackCounter++);
	    }

	    // "pvMSpvMS"
	    private static final long FREE_MARK = 0x70764D5370764D53L;

	    // TODO out-of-order or duplciate fragments will recreate buffer
	    private FragmentationBufferEntry getFragmentationBufferEntry(long seqNo, int dataSize, int fragmentSize)
	    {
	    	FragmentationBufferEntry entry = fragmentTable.get(seqNo);
	    	if (entry != null)
	    		return entry;
	    	
	    	// take next
	    	// TODO take free first, then oldest non-free
	    	entry = fragmentationBuffers.get(fragmentationBuffersIndex);
	    	fragmentationBuffersIndex = (fragmentationBuffersIndex + 1) % fragmentationBuffers.size();
	    	if (entry.seqNo != 0)
	    		fragmentTable.remove(entry.seqNo);
	    	
	    	entry.reset(seqNo, dataSize, fragmentSize);
	    	fragmentTable.put(seqNo, entry);
	    	
	    	return entry;
	    }

	    private class FragmentationBufferEntry {
	    	final ByteBuffer buffer;
	    	long seqNo = 0;
	    	int fragmentSize;
	    	int fragments;
	    	int fragmentsReceived;
	    	
	    	public FragmentationBufferEntry(ByteBuffer buffer) {
	    		this.buffer = buffer;
	    	}
	    	
	    	public void reset(long seqNo, int dataSize, int fragmentSize)
	    	{
		    	if (dataSize > buffer.capacity())
		    		throw new RuntimeException("dataSize > buffer.capacity()");	// TODO different exception

		    	this.seqNo = seqNo;
		    	this.fragmentSize = fragmentSize;
		    	this.fragments = dataSize / fragmentSize + (((dataSize % fragmentSize) != 0) ? 1 : 0);
	    		this.fragmentsReceived = 0;

	    		buffer.limit(dataSize);
	    		
		    	// buffer initialization
		    	int pos = 0;
		    	while (pos < dataSize)
		    	{
		    		buffer.putLong(pos, FREE_MARK);
		    		pos += fragmentSize;
		    	}
	    		
	    	}
	    	
	    	public boolean addFragment(int fragmentStartingNum, ByteBuffer fragmentData)
	    	{
	    		if (fragmentStartingNum > fragments)
	    			throw new IndexOutOfBoundsException("fragmentStartingNum > fragments"); // TODO log!!!
//	    			return false;
	    			
		    	int bufferPosition = (fragmentStartingNum - 1) * fragmentSize;		// starts from 1 on...
		    	if (buffer.getLong(bufferPosition) != FREE_MARK)
		    		return false;			// duplicate fragment received
		    	
		    	// else copy all data to buffer @ bufferPosition
		    	// and increment received fragment count
		    	buffer.position(bufferPosition);
		    	buffer.put(fragmentData);
		    	fragmentsReceived++;
		    	
		    	// all fragments received?
		    	if (fragmentsReceived == fragments)
		    	{
		    		buffer.position(0);
		    		
		    		// TODO optinal behaviour (via ParamList)
		    		ackSNReception(seqNo);
		    		
		    		return true;
		    	}
		    	else
		    		return false;
	    	}
	    	
	    	public void release()
	    	{
	    		fragmentTable.remove(seqNo);
	    		seqNo = 0;
	    	}
	    	
	    }
	    
	    private int lastHeartbeatCount = Integer.MIN_VALUE;
	    private int lastAckNackCount = Integer.MIN_VALUE;
	    
	    private final SequenceNumberSet readerSNState = new SequenceNumberSet();
	    private long lastReceivedSequenceNumber = 0;
	    private long lastKnownSequenceNumber = 0;
	    
	    // sn < ignoreSequenceNumberPrior are ignored
	    private long ignoreSequenceNumbersPrior = 0;
	    
	    
	    // TODO find better (array based) BST, or at least TreeSet<long>
	    private final TreeSet<Long> missingSequenceNumbers = new TreeSet<Long>();
	    
	    private long lastHeartbeatLastSN = 0;
	    
	    private static final int ACKNACK_MISSING_COUNT_THRESHOLD = 64;
	    private long lastAckNackTimestamp = Long.MIN_VALUE;
	    
	    // TODO initialize (message header only once), TODO calculate max size (72?)
	    private final ByteBuffer ackNackBuffer = ByteBuffer.allocate(128);
	    private boolean sendAckNackResponse()
	    {
	    	//System.out.println("missing SN count:" + missingSequenceNumbers.size());
	    	if (missingSequenceNumbers.isEmpty())
	    	{
	    		// we ACK all sequence numbers until lastReceivedSequenceNumber
		    	readerSNState.reset(lastReceivedSequenceNumber + 1);
	    	}
	    	else
	    	{
	    		long first = missingSequenceNumbers.first();
		    	readerSNState.reset(first);
		    	for (Long sn : missingSequenceNumbers)
		    	{
		    		if (sn - first >= 255)			// TODO constant
		    		{
		    			// TODO imagine this case 100, 405...500
		    			// readerSNState can report only 100!
		    			// send NACK only message
		    			break;
		    		}
		    		
		    		readerSNState.set(sn); 
		    	}
		    	// TODO what if we have more
	    	}
	    	
	    	ackNackBuffer.clear();
	    	addMessageHeader(ackNackBuffer);
	    	addAckNackSubmessage(ackNackBuffer, readerSNState);
	    	ackNackBuffer.flip();

	    	// TODO
		    try {
				discoveryUnicastChannel.send(ackNackBuffer, receiver.receivedFrom);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
		    
	    	lastAckNackTimestamp = System.currentTimeMillis();
	    	
	    	return true;
	    }
	    
	    private void checkAckNackCondition()
	    {
	    	checkAckNackCondition(false);
	    }

	    private void checkAckNackCondition(boolean onlyTimeLimitCheck)
	    {
	    	if (onlyTimeLimitCheck)
	    	{
		    	// TODO  check
	    		sendAckNackResponse();
	    		
	    		return;
	    	}
	    	
	    	//System.out.println("new missing: " + missingSequenceNumbers);
	    	
	    	// TODO limit time (frequency, period)
	    	
	    	//if (missingSequenceNumbers.size() >= ACKNACK_MISSING_COUNT_THRESHOLD)
	    		sendAckNackResponse();		// request???
	    }
	    
	    
	    

	    // TODO preinitialize, etc.
	    private final ByteBuffer ackBuffer = ByteBuffer.allocate(64);
	    private boolean ackSNReception(long ackSenNo)
	    {
	    	System.out.println(ackSenNo);
	    	ackBuffer.clear();
	    	addMessageHeader(ackBuffer);
	    	addAckSubmessage(ackBuffer, ackSenNo);
	    	ackBuffer.flip();

	    	// TODO
		    try {
				discoveryUnicastChannel.send(ackBuffer, receiver.receivedFrom);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return false;
			}
	    	return true;
	    }
	    
	    
	    
	    
	    // not thread-safe
	    public boolean processMessage(SocketAddress receivedFrom, ByteBuffer buffer)
		{
			receiver.reset();
			receiver.receivedFrom = receivedFrom;
			
			if (buffer.remaining() < Protocol.RTPS_HEADER_SIZE)
			{
				stats.messageToSmall++;
				return false;
			}
			
			// header fields consist of octet arrays, use big endian to read it
			// in C/C++ ntoh methods would be used
			buffer.order(ByteOrder.BIG_ENDIAN);
			
			// read message header 
			int protocolId = buffer.getInt();
			receiver.sourceVersion = buffer.getShort();
			receiver.sourceVendorId = buffer.getShort();
			buffer.get(receiver.sourceGuidPrefix);
	
			// check protocolId
			if (protocolId != Protocol.ProtocolId.PVDS_VALUE)
			{
				stats.nonRTPSMessage++;
				return false;
			}
	
			// check version
			if (receiver.sourceVersion != ProtocolVersion.PROTOCOLVERSION_2_1)
			{
				stats.versionMismatch++;
				return false;
			}
			
			// check vendor
			if (receiver.sourceVendorId != VendorId.PVDS_VENDORID)
			{
				stats.vendorMismatch++;
				return false;
			}
			
			// process submessages
			int remaining;
			while ((remaining = buffer.remaining()) > 0)
			{
				if (remaining < Protocol.RTPS_SUBMESSAGE_HEADER_SIZE)
				{
					stats.invalidSubmessageSize++;
					return false;
				}
					
				// check aligment
				if (buffer.position() % Protocol.RTPS_SUBMESSAGE_ALIGNMENT != 0)
				{
					stats.submesssageAlignmentMismatch++;
					return false;
				}
				
				// read submessage header
				receiver.submessageId = buffer.get();
				receiver.submessageFlags = buffer.get();
				
				// apply endianess
				ByteOrder endianess = (receiver.submessageFlags & 0x01) == 0x01 ?
											ByteOrder.LITTLE_ENDIAN :
											ByteOrder.BIG_ENDIAN;
				buffer.order(endianess);
				
				// read submessage size (octetsToNextHeader)
				receiver.submessageSize = buffer.getShort() & 0xFFFF;

		        // "jumbogram" condition: octetsToNextHeader == 0 for all except PAD and INFO_TS
				//
				// this submessage is the last submessage in the message and
				// extends up to the end of the message
		        // in case the octetsToNextHeader==0 and the kind of submessage is PAD or INFO_TS,
		        // the next submessage header starts immediately after the current submessage header OR
		        // the PAD or INFO_TS is the last submessage in the message
		        if (receiver.submessageSize == 0 &&
		        	(receiver.submessageId != SubmessageHeader.RTPS_INFO_TS &&
		        	 receiver.submessageId != SubmessageHeader.RTPS_PAD))
		        {
		        	receiver.submessageSize = buffer.remaining();
		        }
		        else if (buffer.remaining() < receiver.submessageSize)
		        {
		        	stats.invalidSubmessageSize++;
		        	return false;
		        }
				
				// min submessage size check
				if (receiver.submessageSize < Protocol.RTPS_SUBMESSAGE_SIZE_MIN)
				{
					stats.invalidSubmessageSize++;
					return false;
				}

				int submessageDataStartPosition = buffer.position();
	
		        stats.submessageType[(receiver.submessageId & 0xFF)]++;
	
				switch (receiver.submessageId) {
	
				case SubmessageHeader.RTPS_DATA:
				case SubmessageHeader.RTPS_DATA_FRAG:
	
					// extraFlags (not used)
					// do not reorder flags (uncomment when used)
					//buffer.order(ByteOrder.BIG_ENDIAN);
					buffer.getShort();
					//buffer.order(endianess);
					
					int octetsToInlineQos = buffer.getShort() & 0xFFFF;

					buffer.order(ByteOrder.BIG_ENDIAN);
					// entityId is octet[3] + octet
					receiver.readerId = buffer.getInt();
					receiver.writerId = buffer.getInt();
					buffer.order(endianess);
					
					long seqNo = buffer.getLong();

					stats.receivedSN++;
					//System.out.println("rx: " + seqNo);
					
					if (seqNo < ignoreSequenceNumbersPrior)
					{
						stats.ignoredSN++;
						break;
					}

					
					if (seqNo > lastReceivedSequenceNumber)
					{
						// mark [lastReceivedSequenceNumber + 1, seqNo - 1] as missing
						long newMissingSN = 0;
						for (long sn = lastReceivedSequenceNumber + 1; sn < seqNo; sn++)
							if (missingSequenceNumbers.add(sn))
								newMissingSN++;
						
						if (newMissingSN > 0)
						{
							stats.missedSN += newMissingSN;
							checkAckNackCondition();
						}
						
						lastReceivedSequenceNumber = seqNo;
					}
					
					if (seqNo > lastKnownSequenceNumber)
						lastKnownSequenceNumber = seqNo;
					else
					{
						// might be missing SN, try to remove
						boolean missed = missingSequenceNumbers.remove(seqNo);
						if (missed)
							stats.recoveredSN++;
					}
					
					boolean isData = (receiver.submessageId == SubmessageHeader.RTPS_DATA);
					if (isData)
					{
						// jump to inline InlineQoS (or Data)
						buffer.position(buffer.position() + octetsToInlineQos - 16);	// 16 = 4+4+8
						
						// InlineQoS present
						boolean flagQ = (receiver.submessageFlags & 0x02) == 0x02;
						// Data present
						boolean flagD = (receiver.submessageFlags & 0x04) == 0x04;
						
						/*
						// Key present
						boolean flagK = (receiver.submessageFlags & 0x08) == 0x08;
						*/
						
						if (flagQ)
						{
							// ParameterList inlineQos
							// TODO
						}
						
						// TODO resolve appropriate reader
						// and decode data there
						
						// flagD and flagK are exclusive
						if (flagD)
						{
							// Data

							/*
							int serializedDataLength = 
								submessageDataStartPosition + receiver.submessageSize - buffer.position();
							*/
							
							newData(buffer);
						}
						/*
						else if (flagK)
						{
							// Key
						}
						*/
						
					}
					else		// DataFrag
					{
						// TODO way to identify dataFrag !!!
						
					    // fragmentStartingNum (unsigned integer, starting from 1)
						int fragmentStartingNum = buffer.getInt();

						// fragmentsInSubmessage (unsigned short)
						int fragmentsInSubmessage = buffer.getShort() & 0xFFFF;

					    // fragmentSize (unsigned short)
						int fragmentSize = buffer.getShort() & 0xFFFF;
					    
					    // sampleSize or dataSize (unsigned integer)
						int dataSize = buffer.getInt();
						
						// jump to inline InlineQoS (or Data)
						buffer.position(buffer.position() + octetsToInlineQos - 16);	// 16 = 4+4+8

						long firstFragmentSeqNo = (seqNo - fragmentStartingNum + 1);
						
						FragmentationBufferEntry entry = getFragmentationBufferEntry(firstFragmentSeqNo, dataSize, fragmentSize);
						if (entry != null)
						{
							for (int i = 0; i < fragmentsInSubmessage; i++)
							{
								if (entry.addFragment(fragmentStartingNum, buffer))
								{
									newData(entry.buffer);
									entry.release();
									break;
								}
								fragmentStartingNum++;
							}
						}
					}
					

					break;

				case SubmessageHeader.RTPS_HEARTBEAT:
					{
						buffer.order(ByteOrder.BIG_ENDIAN);
						// entityId is octet[3] + octet
						receiver.readerId = buffer.getInt();
						receiver.writerId = buffer.getInt();
						buffer.order(endianess);
						
						long firstSN = buffer.getLong();
						long lastSN = buffer.getLong();
						
						int count = buffer.getInt();
						
						if (firstSN <= 0 || lastSN <= 0 || lastSN < firstSN)
						{
							stats.invalidMessage++;
							return false;
						}
						
						// TODO warp
						if (count > lastHeartbeatCount)
						{
							lastHeartbeatCount = count;
						
							// TODO remove
							//System.out.println("HEARTBEAT: " + firstSN + " -> " + lastSN + " | " + count);
							
							if (lastSN > lastKnownSequenceNumber)
								lastKnownSequenceNumber = lastSN;
							
							// remove ones that are not available anymore
							long lostSNCount = 0;
							long minMissingSN = Math.max(firstSN, ignoreSequenceNumbersPrior);
							while (!missingSequenceNumbers.isEmpty() && missingSequenceNumbers.first() < minMissingSN)
							{
								/*long lostSN = */missingSequenceNumbers.pollFirst();
								lostSNCount++;
								
								// TODO
								
								// cancel fragments that will never be completed
								// FragmentationBufferEntry.seqNo <= lostSN < FragmentationBufferEntry.{seqNo + fragments}
								// take care of O(n**2) compexity, move out of this loop and use range-lists
								
								// set ignoreSequenceNumbersPrior to FragmentationBufferEntry.{seqNo + fragments} if
								// the fragment is part of lost message (see description above) and the oldest fragment
							}
							stats.lostSN += lostSNCount;

							// add new available (from firstSN on), missed sequence numbers
							long newMissingSN = 0;
							for (long sn = Math.max(ignoreSequenceNumbersPrior, Math.max(lastReceivedSequenceNumber + 1, firstSN)); sn <= lastSN; sn++)
								if (missingSequenceNumbers.add(sn))
									newMissingSN++;
							
							stats.missedSN += newMissingSN;
							
							// FinalFlag flag (require response)
							boolean flagF = (receiver.submessageFlags & 0x02) == 0x02;
							if (flagF)
								sendAckNackResponse();
							// no new data on writer side and we have some SN missing - request them!
							else if (lastHeartbeatLastSN == lastSN && missingSequenceNumbers.size() > 0)
								checkAckNackCondition(true);
							else if (newMissingSN > 0)
								checkAckNackCondition();

							// LivelinessFlag
							//boolean flagL = (receiver.submessageFlags & 0x04) == 0x04;
	
							lastHeartbeatLastSN = lastSN;

							// TODO remove
							//System.out.println("\t" + missingSequenceNumbers);
							//System.out.println("\tmissed   : " + stats.missedSN);
							//System.out.println("\treceived : " + stats.receivedSN + " (" + (100*stats.receivedSN/(stats.receivedSN+stats.missedSN))+ "%)");
							//System.out.println("\tlost     : " + stats.lostSN);
							//System.out.println("\trecovered: " + stats.recoveredSN);
						}
					}
					break;
					
				case SubmessageHeader.RTPS_ACKNACK:
				{
					buffer.order(ByteOrder.BIG_ENDIAN);
					// entityId is octet[3] + octet
					receiver.readerId = buffer.getInt();
					receiver.writerId = buffer.getInt();
					buffer.order(endianess);

					if (!readerSNState.deserialize(buffer))
					{
						stats.invalidMessage++;
						return false;
					}
					
					int count = buffer.getInt();

					// TODO warp
					if (count > lastAckNackCount)
					{
						lastAckNackCount = count;
						
						nack(readerSNState, (InetSocketAddress)receiver.receivedFrom);
						
						//System.out.println("ACKNACK: " + readerSNState + " | " + count);
						
					}
				}
				break;

				// temporary, maybe there is a better way (using HEARTBEAT and ACKNACK)
				case SubmessageHeader.PVDS_ACK:
				{
					buffer.order(ByteOrder.BIG_ENDIAN);
					// entityId is octet[3] + octet
					receiver.readerId = buffer.getInt();
					receiver.writerId = buffer.getInt();
					buffer.order(endianess);
					
					long ackSeqNo = buffer.getLong();
					
					if (ackSeqNo <= 0)
					{
						stats.invalidMessage++;
						return false;
					}
					
					int count = buffer.getInt();
					
					ack(ackSeqNo);
				}
				break;
				
				default:
					stats.unknownSubmessage++;
					return false;
				}
	
		        // jump to next submessage position
		        buffer.position(submessageDataStartPosition + receiver.submessageSize);	// exception?
			}
			
			stats.validMessage++;
			
			return true;
		}
	    
	    // receiver side
	    private final Object newDataMonitor = new Object();
	    ByteBuffer newDataBuffer;
	    private void newData(ByteBuffer buffer)
	    {
			//System.out.println("Received data: " + buffer.remaining() + " bytes");
			synchronized (newDataMonitor) {
				newDataBuffer = buffer;
				newDataMonitor.notifyAll();
			}
	    }
	    
	    // transmitter side
	    private final Object ackMonitor = new Object();
	    private void ack(long ackSeqNo)
	    {
			System.out.println(ackSeqNo);
			synchronized (ackMonitor) {
				receivedCount++;
				if (receivedCount >= expectedReceivedCount)
					ackMonitor.notifyAll();
			}
	    }

	    int receivedCount = 0;
	    int expectedReceivedCount = 0;
	    public int waitUntilReceived(int count, long timeout) throws InterruptedException
	    {
	    	synchronized (ackMonitor) {
	    		if (receivedCount >= count)
	    			return receivedCount;
	    		expectedReceivedCount = count;
	    		ackMonitor.wait(timeout);
	    		int retVal = receivedCount;
	    		receivedCount = 0;
	    		return retVal;
	    	}
	    }

	    public PVField waitForNewData(PVField data, long timeout) throws InterruptedException
	    {
	    	synchronized (newDataMonitor) {
	    		if (newDataBuffer == null)
	    			newDataMonitor.wait(timeout);
	    		if (newDataBuffer != null)
	    		{
	    			data.deserialize(newDataBuffer, NOOP_DESERIALIZABLE_CONTROL);
	    			newDataBuffer = null;
	    		}
	    		return data;
	    	}
	    }
	    
	    protected void nack(SequenceNumberSet readerSNState, InetSocketAddress recoveryAddress)
	    {
	    	// noop from receiver
	    }
	    
	}
	
	//static class RTPSMessageTransmitter extends RTPSMessageEndPoint implements SerializableControl {
	static class RTPSMessageTransmitter extends RTPSMessageReceiver implements SerializableControl {

		// this instance (writer) EntityId;
		final int writerId;
		
	    // TODO to be configurable

		// NOTE: Giga means 10^9 (not 1024^3)
	    double udpTxRateGbitPerSec = Double.valueOf(System.getProperty("RATE", "0.96")); // TODO !!
	    final int MESSAGE_ALIGN = 32;
	    final int MAX_PACKET_SIZE_BYTES = (8000 / MESSAGE_ALIGN) * MESSAGE_ALIGN;
	    long delay_ns = (long)(MAX_PACKET_SIZE_BYTES * 8 / udpTxRateGbitPerSec);
	
	    // TODO configurable
	    final int SEND_BUFFER_SIZE_KB = 3*10*1024;		// TODO
	    final int MIN_SEND_BUFFER_PACKETS = 2;
	    
	    final AtomicInteger resendRequestsPending = new AtomicInteger(0);
	    
	    class BufferEntry {
	    	final ByteBuffer buffer;

	    	long sequenceNo;
	    	InetSocketAddress sendAddress;
	    	
	    	// 0 - none, 1 - unicast, > 1 - multicast
	    	final AtomicInteger resendRequests = new AtomicInteger(0);
	    	final AtomicReference<InetSocketAddress> resendUnicastAddress = new AtomicReference<InetSocketAddress>();
	    	
	    	public BufferEntry(ByteBuffer buffer) {
	    		this.buffer = buffer;
	    	}
	    	
	    	// thread-safe, must never lock (no locks)
	    	public void resendRequest(InetSocketAddress address)
	    	{
	    		int resendRequestsCount = resendRequests.getAndIncrement();
	    		if (resendRequestsCount == 0)
	    			resendUnicastAddress.set(address);
	    		// duplicate call of same reader, this check is here
	    		// to avoid the same reader increment resendRequests > 1
	    		// and as consequence resend could not be done as unicast
// TODO this causes problems
//	    		else if (resendUnicastAddress.get() == address)
//	    			resendRequests.compareAndSet(resendRequestsCount + 1, resendRequestsCount);
	    		
	    		// NOTE: sum of all resendRequest might not be same as resendRequestsPending for a moment
	    		// we do not care for multiple request of the same reader
	    		resendRequestsPending.incrementAndGet();
	    	}
	    	
	    	// NOTE: no call to resendRequest() must be made after this method is called, until message is actually sent
	    	public synchronized ByteBuffer prepare(long sequenceId, InetSocketAddress sendAddress)
	    	{
	    		// invalidate all resend requests
	    		resendRequestsPending.addAndGet(-resendRequests.getAndSet(0));
	    		resendUnicastAddress.set(null);

	    		this.sequenceNo = sequenceId;
	    		this.sendAddress = sendAddress;
	    		
	    		buffer.clear();
	    		
	    		return buffer;
	    	}

	    	/// ???
	    	public void beforeSend()
	    	{
	    		resendRequestsPending.addAndGet(-resendRequests.getAndSet(0));
	    	}

	    	/// ???
	    	public void afterSend()
	    	{
	    		resendRequestsPending.addAndGet(-resendRequests.getAndSet(0));
	    	}
	    }
	    

	    final ArrayBlockingQueue<BufferEntry> freeQueue;
	    final ArrayBlockingQueue<BufferEntry> sendQueue;
	    
	    final ConcurrentHashMap<Long, BufferEntry> recoverMap = new ConcurrentHashMap<Long, BufferEntry>();

	    // TODO for test
	    final InetSocketAddress multicastAddress;

		final ByteBuffer serializationBuffer;

	    public RTPSMessageTransmitter(String multicastNIF, int domainId, int writerId) throws Throwable {
			//super(multicastNIF, domainId);
			super(multicastNIF, domainId, 0, null);
			this.writerId = writerId;

		    // sender setup
		    discoveryUnicastChannel.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true);
		    discoveryUnicastChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, nif);
		    multicastAddress = new InetSocketAddress(discoveryMulticastGroup, discoveryMulticastPort);

		    int bufferPacketsCount = (int)(SEND_BUFFER_SIZE_KB * 1024L / (MAX_PACKET_SIZE_BYTES+SPARE_BUFFER_SIZE));
		    if (bufferPacketsCount < MIN_SEND_BUFFER_PACKETS)
		    	throw new RuntimeException("send buffer to small");
		    
		    freeQueue = new ArrayBlockingQueue<BufferEntry>(bufferPacketsCount);
		    sendQueue = new ArrayBlockingQueue<BufferEntry>(bufferPacketsCount);

		    ByteBuffer buffer = ByteBuffer.allocate(bufferPacketsCount*(MAX_PACKET_SIZE_BYTES+SPARE_BUFFER_SIZE));
		    serializationBuffer = buffer.duplicate();
		
		    //BufferEntry[] bufferPackets = new BufferEntry[bufferPacketsCount];
		    int pos = 0;
		    for (int i = 0; i < bufferPacketsCount; i++)
		    {
		    	buffer.limit(pos + MAX_PACKET_SIZE_BYTES);
		    	buffer.position(pos);
		    	pos += MAX_PACKET_SIZE_BYTES+SPARE_BUFFER_SIZE;

		    	//bufferPackets[i] = new BufferEntry(buffer.slice());
		    	freeQueue.add(new BufferEntry(buffer.slice()));
		    }
		    
		    System.out.println("Transmitter: buffer size = " + bufferPacketsCount + " packets of " + MAX_PACKET_SIZE_BYTES + 
		    				   " bytes, rate limit: " + udpTxRateGbitPerSec + "Gbit/sec (period: " + delay_ns + " ns)");

		}
	    
	    // TODO activeBuffers can have message header initialized  only once
	    /*
	    public void initializeMessage(ByteBuffer buffer)
	    {
		    // MessageHeader
	    	buffer.putLong(Protocol.HEADER_NO_GUID);
		    buffer.put(guidPrefix.value);
	    }
	    
	    public void resetMessage(ByteBuffer buffer)
	    {
	    	// MessageHeader is static (fixed)
	    	buffer.position(Protocol.RTPS_HEADER_SIZE);
	    	buffer.limit(buffer.capacity());
	    }
	    */
	    

	    
	    private static final int DATA_SUBMESSAGE_NO_QOS_PREFIX_LEN = 24;
	    
	    // no fragmentation
	    public void addDataSubmessage(ByteBuffer buffer, PVField data, int dataSize)
	    {
	    	// big endian, data flag
	    	addSubmessageHeader(buffer, SubmessageHeader.RTPS_DATA, (byte)0x04, 0x0000);
		    int octetsToNextHeaderPos = buffer.position() - 2;

		    // extraFlags
		    buffer.putShort((short)0x0000);
		    // octetsToInlineQoS
		    buffer.putShort((short)0x0010);		// 16 = 4+4+8
		    
		    // readerId
		    buffer.putInt(0);		
		    // writerId
		    buffer.putInt(writerId);
		    
		    // writerSN
		    buffer.putLong(newWriterSequenceNumber());
		    
		    // InlineQoS TODO
		    
		    // Data
		    // must fit this buffer, this is not DATA_FRAG message
		    // TODO use serialize() method that does not checks !!!
		    data.serialize(buffer, NOOP_SERIALIZABLE_CONTROL);
		    
		    // set message size
		    int octetsToNextHeader = buffer.position() - octetsToNextHeaderPos - 2;
		    buffer.putShort(octetsToNextHeaderPos, (short)(octetsToNextHeader & 0xFFFF));
	    }

	    private int heartbeatCounter = 0;
	    
	    // tells what is currently in send buffers
	    
	    // send it:
	    //    - when no data to send
	    //           - (immediately when queue gets empty (*)) 
	    //           - periodically with back-off (if all acked all, no need)
	    //    - every N messages
	    //    (- manually)
	    // * every message that sends seqNo, in a way sends heartbeat.lastSN
	    public void addHeartbeatSubmessage(ByteBuffer buffer, long firstSN, long lastSN)
	    {
	    	// big endian flag
	    	addSubmessageHeader(buffer, SubmessageHeader.RTPS_HEARTBEAT, (byte)0x00, 28);
		    
		    // readerId
		    buffer.putInt(0);		
		    // writerId
		    buffer.putInt(writerId);
		    
		    // firstSN
		    buffer.putLong(firstSN);
		    // lastSN
		    buffer.putLong(lastSN);
		    
		    // count
		    buffer.putInt(heartbeatCounter++);
	    }

	    private int fragmentStartingNum = 1;
	    private int fragmentTotalSize = 0;
	    private int fragmentSize = 0;
	    private int fragmentDataLeft = 0;
	    
	    // fragmentation
	    public void addDataFragSubmessageHeader(ByteBuffer buffer)
	    {
	    	boolean firstFragment = (fragmentStartingNum == 1);
	    	
	    	// big endian flag
	    	addSubmessageHeader(buffer, SubmessageHeader.RTPS_DATA_FRAG, (byte)0x00, 0x0000);

		    // extraFlags
		    buffer.putShort((short)0x0000);
		    // octetsToInlineQoS
		    buffer.putShort((short)0x0010);		// 16 = 4+4+8
		    
		    // readerId
		    buffer.putInt(0);		
		    // writerId
		    buffer.putInt(writerId);
		    
		    // writerSN
		    buffer.putLong(newWriterSequenceNumber());
		    
		    // fragmentStartingNum (unsigned integer, starting from 1)
		    buffer.putInt(fragmentStartingNum++);
		    
		    // fragmentsInSubmessage (unsigned short)
		    buffer.putShort((short)1);
		    
		    // fragmentSize (unsigned short)
		    // should not change, last actually fragmented data size is < fragmentSize
		    if (firstFragment)
		    {
		    	fragmentSize = buffer.remaining() - 6;	// - fragmentSize - dataSize (TODO - InlineQoS)
			    fragmentSize = Math.min(fragmentDataLeft, fragmentSize);
		    }
		    buffer.putShort((short)fragmentSize);
		    
		    fragmentDataLeft -= fragmentSize;

		    // sampleSize (unsigned integer)
		    buffer.putInt(fragmentTotalSize);
		    
		    // InlineQoS TODO

		    // data payload comes next
		    
		    // octetsToNextHeader is left to 0 (until end of the message)
		    // this means this DataFrag message must be the last message 
	    }

	    // fragmentation
	    public void addDataFragSubmessage(ByteBuffer buffer, PVField data, int dataSize)
	    {
	    	fragmentTotalSize = fragmentDataLeft = dataSize;
	    	fragmentSize = 0;		// to be initialized later
	    	fragmentStartingNum = 1;
	    	
	    	addDataFragSubmessageHeader(buffer);
	    			    
		    data.serialize(buffer, this);
	    }

	    /*
	    // TODO tmp
	    public void addAnnounceSubmessage(ByteBuffer buffer, int changeCount, Locator unicastEndpoint,
	    		int entitiesCount, BloomFilter<String> filter)
	    {
	    	addSubmessageHeader(buffer, SubmessageHeader.PVDS_ANNOUNCE, (byte)0x00, 0x0000);
		    int octetsToNextHeaderPos = buffer.position() - 2;
		    
		    // unicast discovery locator
		    unicastEndpoint.serialize(buffer);
		    
		    // change count 
		    buffer.putInt(changeCount);

		    // service locators // TODO
		    // none for now
		    buffer.putInt(0);
		    
		    // # of discoverable entities (-1 not supported, or dynamic)
		    buffer.putInt(entitiesCount);
		    
		    if (entitiesCount > 0)
		    {
			    buffer.putInt(filter.k());
			    buffer.putInt(filter.m());
			    long[] bitArray = filter.bitSet().getBitArray();
			    buffer.asLongBuffer().put(bitArray);
			    buffer.position(buffer.position() + bitArray.length * 8);
		    }
		    
		    // set message size (generic code) for now
		    int octetsToNextHeader = buffer.position() - octetsToNextHeaderPos - 2;
		    buffer.putShort(octetsToNextHeaderPos, (short)(octetsToNextHeader & 0xFFFF));
	    }
	    */
	    
	    // recovery marker
	    protected final void nack(SequenceNumberSet readerSNState, InetSocketAddress recoveryAddress)
	    {
	    	long base = readerSNState.bitmapBase;
	    	BitSet bitSet = readerSNState.bitmap;
	        int i = -1;
            for (i = bitSet.nextSetBit(i+1); i >= 0; i = bitSet.nextSetBit(i+1))
            {
            	long nackedSN = base + i;
                int endOfRun = bitSet.nextClearBit(i);
                do { 
                	
    	    		BufferEntry be = recoverMap.get(nackedSN);
    	    		if (be != null)
    	    		{
    	    			be.resendRequest(recoveryAddress);
    	    		}
    	    		else
    	    		{
    	    			// recovery works in FIFO manner, therefore also none
    	    			// of the subsequent packets will be available
    	    			return;
    	    		}
                	
                	nackedSN++;
                } while (++i < endOfRun);
            }
	    }
	    
	    // TODO preinitialize, can be fixed
	    final static ByteBuffer heartbeatBuffer = ByteBuffer.allocate(64);
	    private final boolean sendHeartbeatMessage() throws IOException
	    {
	    	// check if the message is valid (e.g. no messages sent or if lastOverridenSeqNo == lastSentSeqNo)
	    	long firstSN = lastOverridenSeqNo.get() + 1;
	   		if (lastSentSeqNo == 0 || firstSN >= lastSentSeqNo)
	   			return false;
	   		heartbeatBuffer.clear();

	   		addMessageHeader(heartbeatBuffer);
    		addHeartbeatSubmessage(heartbeatBuffer, firstSN, lastSentSeqNo);
    		heartbeatBuffer.flip();
    		// TODO !!!
		    discoveryUnicastChannel.send(heartbeatBuffer, multicastAddress);

		    return true;
	    }
	    
	    // TODO make configurable
	    private static long MIN_HEARTBEAT_TIMEOUT_MS = 1;
	    private static long MAX_HEARTBEAT_TIMEOUT_MS = 64*1024;
	    private static long HEARTBEAT_PERIOD_MESSAGES = 100;		// send every 100 messages (if not sent otherwise)
	    
	    // TODO implement these
	    private AtomicLong oldestACKedSequenceNumber = new AtomicLong(0);
	    private AtomicInteger readerCount = new AtomicInteger(1);

	    // TODO heartbeat, acknack are not exclided (do not sent lastSendTime)
	    private long lastSendTime = 0;
	    
	    public void sendProcess() throws IOException, InterruptedException
	    {
	    	int messagesSinceLastHeartbeat = 0;
	    	long heartbeatTimeout = MAX_HEARTBEAT_TIMEOUT_MS; 
	    	
		    // sender
		    while (true)
		    {
			    BufferEntry be = sendQueue.poll();
			    if (be == null)
			    {
				    //System.out.println("resendRequestsPending: " + resendRequestsPending.get());
			    	if (resendRequestsPending.get() > 0)
			    	{
			    		resendProcess();
				    	heartbeatTimeout = MIN_HEARTBEAT_TIMEOUT_MS;
			    		continue;
			    	}
			    	
			    	be = sendQueue.poll(heartbeatTimeout, TimeUnit.MILLISECONDS);
			    	if (be == null)
			    	{
			    		if (readerCount.get() > 0 &&
			    			oldestACKedSequenceNumber.get() < lastSentSeqNo)
			    		{
			    			if (sendHeartbeatMessage())
			    				messagesSinceLastHeartbeat = 0;
			    		}
			    		heartbeatTimeout = Math.min(heartbeatTimeout << 1, MAX_HEARTBEAT_TIMEOUT_MS);
			    		continue;
			    	}
			    }
			    
		    	heartbeatTimeout = MIN_HEARTBEAT_TIMEOUT_MS;

		    	// TODO we need at least 1us precision?

		    	// while-loop only version
			    // does not work well on single-core CPUs
/*
			    long endTime = lastSendTime + delay_ns;
				long sleep_ns;
			    while (true)
			    {
					lastSendTime = System.nanoTime();
			    	sleep_ns = endTime - lastSendTime;
			    	if (sleep_ns < 1000)		// TODO
			    		break;
			    	else if (sleep_ns > 100000)	// TODO on linux this is ~2000
				    	Thread.sleep(0);
			    	//	Thread.yield();
			    }*/
				long endTime = lastSendTime + delay_ns;
				while (endTime - System.nanoTime() > 0);
				
				lastSendTime = System.nanoTime();	// TODO adjust nanoTime() overhead?

			    //System.out.println(sleep_ns);

			    if (be.sequenceNo != 0)
			    {
			    	recoverMap.put(be.sequenceNo, be);
			    	lastSentSeqNo = be.sequenceNo;
			    	
			    		// TODO remove
			    	//System.out.println("tx: " + be.sequenceNo);
			    }
			    be.buffer.flip();
			    discoveryUnicastChannel.send(be.buffer, be.sendAddress);
			    freeQueue.put(be);

		    	messagesSinceLastHeartbeat++;
		    	if (messagesSinceLastHeartbeat > HEARTBEAT_PERIOD_MESSAGES)
		    	{
		    		// TODO try to piggyback
	    			if (sendHeartbeatMessage())
	    				messagesSinceLastHeartbeat = 0;
		    	}

		    }
	    }
	    
	    private void resendProcess() throws IOException, InterruptedException
	    {
	    	int messagesSinceLastHeartbeat = 0;

	    	for (BufferEntry be : freeQueue)
	    	{
			    int resendRequestCount = be.resendRequests.get();
			    if (resendRequestCount > 0)
			    {
				    // TODO we need at least 1us precision?
			    	// we do not want to sleep while lock is held, do it here
				    long endTime = lastSendTime + delay_ns;
					while (endTime - System.nanoTime() > 0);

					// guard buffer from being taken by serialization process
					synchronized (be)
		    		{
						// recheck if not taken by prepareBuffer() method
					    resendRequestCount = be.resendRequests.getAndSet(0);
					    resendRequestsPending.getAndAdd(-resendRequestCount);
					    if (resendRequestCount > 0)
					    {
						    lastSendTime = System.nanoTime();	// TODO adjust nanoTime() overhead?

						    be.buffer.flip();
						    // send on unicast address directly if only one reader is interested in it
						    // TODO commented out since reader is only listening on multicast port
						    //discoveryUnicastChannel.send(be.buffer, (resendRequestCount == 1) ? be.resendUnicastAddress.get() : be.sendAddress);
						    discoveryUnicastChannel.send(be.buffer, be.sendAddress);
		
						    messagesSinceLastHeartbeat++;
					    }
		    		}
	    		}

	    		// do not hold lock while sending heartbeat
	    		// (this is why this block is moved outside sync block above; was just after messagesSinceLastHeartbeat++)
	    		if (messagesSinceLastHeartbeat > HEARTBEAT_PERIOD_MESSAGES)
		    	{
		    		// TODO try to piggyback
	    			if (sendHeartbeatMessage())
	    				messagesSinceLastHeartbeat = 0;
		    	}
	    	}
	    }
	    
	    // NOTE: this differs from RTPS spec (here writerSN changes also for every fragment)
	    // do not put multiple Data/DataFrag message into same message (they will report different ids)
		private final AtomicLong writerSequenceNumber = new AtomicLong(0);
		
		BufferEntry activeBufferEntry;
		ByteBuffer activeBuffer;
		
		// TODO remove
		public ByteBuffer getBuffer()
		{
			return serializationBuffer;
		}
		
		// not thread-safe
	    public void send(PVField data) throws InterruptedException
	    {
	    	int dataSize = getSerializationSize(data);

	    	takeFreeBuffer();
		    
		    addMessageHeader(serializationBuffer);
		    // TODO put all necessary messages here
		    
		    if ((dataSize + DATA_SUBMESSAGE_NO_QOS_PREFIX_LEN) <= MAX_PACKET_SIZE_BYTES)
		    	addDataSubmessage(serializationBuffer, data, dataSize);
		    else
		    	addDataFragSubmessage(serializationBuffer, data, dataSize);

		    sendBuffer();
	    }
	    
	    // TODO
	    public void waitUntilSent() {
	    	while (!sendQueue.isEmpty())
	    		Thread.yield();
	    }
	    
	    private long lastSentSeqNo = 0;
	    private final AtomicLong lastOverridenSeqNo = new AtomicLong(0);
	    
	    private void takeFreeBuffer() throws InterruptedException
	    {
	    	// TODO close existing one?

	    	// TODO !!!
		    activeBufferEntry = freeQueue.poll(1, TimeUnit.SECONDS);
		    // TODO remove?
		    if (activeBufferEntry == null)
		    	throw new RuntimeException("no free buffer");
	    	
		    // TODO option with timeout...
		    
		    long seqNo = activeBufferEntry.sequenceNo;
		    if (seqNo != 0)
		    {
		    	recoverMap.remove(seqNo);
		    	lastOverridenSeqNo.set(seqNo);
		    }

		    // TODO
		    InetSocketAddress sendAddress = multicastAddress;
		    
		    activeBuffer = activeBufferEntry.prepare(0, sendAddress);
		    
		    // note: activeBuffer wraps buffer (serializationBuffer)
		    int offset = activeBuffer.arrayOffset();
		    serializationBuffer.position(0);
		    serializationBuffer.limit(offset + activeBuffer.capacity());
		    serializationBuffer.position(offset);
	    }
	    
	    private final long newWriterSequenceNumber()
	    {
	    	long newWriterSN = writerSequenceNumber.incrementAndGet();
	    	//System.out.println("tx newWriterSN: " + newWriterSN);
	    	activeBufferEntry.sequenceNo = newWriterSN;
	    	return newWriterSN;
	    }
	    
	    private void sendBuffer() throws InterruptedException
	    {
	    	// copy back
		    int offset = activeBuffer.arrayOffset();
	    	activeBuffer.position(serializationBuffer.position() - offset);
	    	activeBuffer.limit(serializationBuffer.limit() - offset);
	    	
		    sendQueue.put(activeBufferEntry);

		    activeBufferEntry = null;
	    	activeBuffer = null;
	    }

		final int SPARE_BUFFER_SIZE = 8;
	    final byte[] spareBuffer = new byte[SPARE_BUFFER_SIZE];
	    private int spareOverflow = 0;
	    
	    private boolean spareEnabled = false;
	    private int preSpareLimit = 0;
	    
		@Override
		public void flushSerializeBuffer() {
			
			try {
				if (spareEnabled)
				{
					spareOverflow = serializationBuffer.position() - preSpareLimit;
					if (spareOverflow < 0)
					{
						spareEnabled = false;
						throw new RuntimeException("serialization logic exception");
					}
					
					// save "overflow"
					serializationBuffer.position(preSpareLimit);
					serializationBuffer.get(spareBuffer);
					
					// fix limit
					serializationBuffer.position(preSpareLimit);
					serializationBuffer.limit(preSpareLimit);
				}
				else
				{
					// not full
					if (serializationBuffer.hasRemaining())
					{
						preSpareLimit = serializationBuffer.limit();
						serializationBuffer.limit(preSpareLimit + SPARE_BUFFER_SIZE);
						spareEnabled = true;
						return;
					}
				}
				
				sendBuffer();
				takeFreeBuffer();
				addMessageHeader(serializationBuffer);
				addDataFragSubmessageHeader(serializationBuffer);
				
				if (spareEnabled && spareOverflow > 0)
				{
					// TODO or not.... check if we have spareOverflow of free buffer...
					serializationBuffer.put(spareBuffer, 0, spareOverflow);
					spareOverflow = 0;
					spareEnabled = false;
				}
					
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}

			
		}

		@Override
		public void ensureBuffer(int size) {
			if (serializationBuffer.remaining() < size)
			{
				flushSerializeBuffer();

				if (serializationBuffer.remaining() < size)
					throw new RuntimeException("requested buffer size exceeds maximum payload size");
			}
		}

		@Override
		public void alignBuffer(int alignment) {
			// TODO 
		}

		@Override
		public void cachedSerialize(Field field, ByteBuffer buffer) {
			// no-cache
			field.serialize(buffer, this);
		}
	    
	}
	
	private final static Structure simpleStructure =
		FieldFactory.getFieldCreate().createFieldBuilder().
			add("value", ScalarType.pvInt).
			createStructure();

	public static final int[] scalarSize = {
		1, // pvBoolean
		1,    // pvByte
		2,   // pvShort
		4,     // pvInt
		8,    // pvLong
		1,   // pvUByte
		2,  // pvUShort
		4,    // pvUInt
		8,   // pvULong
		4,   // pvFloat
		8,  // pvDouble
		0   // pvString
	};

	public static int getSerializationSizeSize(int size)
	{
		if (size == -1)	
			return 1;
		else if (size < 254)
			return 1;
		else
			return 5;
	}
	
	public static int getStringSerializationSize(String str)
	{
		int len = str.length();
		return getSerializationSizeSize(len) + len;
	}
	
	public static int getStructureSerializationSize(PVStructure pvStructure)
	{
		PVField[] pvFields = pvStructure.getPVFields();
		int size = getSerializationSizeSize(pvFields.length);
		for (PVField pvf : pvFields)
			size += getSerializationSize(pvf);
		return size;
	}
	
	public static int getUnionSerializationSize(PVUnion pvUnion)
	{
		PVField value = pvUnion.get();
		if (pvUnion.getUnion().isVariant())
		{
			if (value == null)
				return 1;
			else
			{
				throw new RuntimeException("not supported");
				// TODO 
				//return getSerializationSize(value.getField()) + getSerializationSize(value);
			}
		}
		else
		{
			int selector = pvUnion.getSelectedIndex();
			int size = getSerializationSizeSize(selector);
			if (selector != PVUnion.UNDEFINED_INDEX) 
				size += getSerializationSize(value);
			return size;
		}
	}
	
	// TODO alignment for arrays
	public static int getSerializationSize(PVField pvField)
	{
		Field field = pvField.getField();
		switch (field.getType())
		{
		case scalar:
		{
			ScalarType scalarType = ((Scalar)field).getScalarType();
			if (scalarType == ScalarType.pvString)
				return getStringSerializationSize(((PVString)pvField).get());
			else
				return scalarSize[scalarType.ordinal()];
		}
		case structure:
		{
			return getStructureSerializationSize((PVStructure)pvField);
		}
		case union:
		{
			return getUnionSerializationSize((PVUnion)pvField);
		}
		case scalarArray:
		{
			PVScalarArray pvScalarArray = (PVScalarArray)pvField;
			int arrayLength = pvScalarArray.getLength();
			ScalarType elementType = pvScalarArray.getScalarArray().getElementType();
			if (elementType == ScalarType.pvString)
			{
				StringArrayData sad = new StringArrayData();
				((PVStringArray)pvScalarArray).get(0, arrayLength, sad);
				int size = getSerializationSizeSize(arrayLength);
				for (String s : sad.data)
					size += getStringSerializationSize(s);
				return size;
			}
			else
				return getSerializationSizeSize(arrayLength) + arrayLength * scalarSize[elementType.ordinal()];
		}
		case structureArray:
		{
			PVStructureArray pvStructureArray = (PVStructureArray)pvField;
			int arrayLength = pvStructureArray.getLength();
			int size = getSerializationSizeSize(arrayLength);
			StructureArrayData sad = new StructureArrayData();
			pvStructureArray.get(0, arrayLength, sad);
			for (PVStructure s : sad.data)
				size += 1 + getStructureSerializationSize(s);
			return size;
		}
		case unionArray:
		{
			PVUnionArray pvUnionArray = (PVUnionArray)pvField;
			int arrayLength = pvUnionArray.getLength();
			int size = getSerializationSizeSize(arrayLength);
			UnionArrayData sad = new UnionArrayData();
			pvUnionArray.get(0, arrayLength, sad);
			for (PVUnion s : sad.data)
				size += 1 + getUnionSerializationSize(s);
			return size;
		}
		default:
			throw new IllegalStateException("unsupported type " + pvField.getField());
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Throwable {
		
        //ExecutorService actorsThreadPool = Executors.newCachedThreadPool();
        //Actors actors = new Actors(actorsThreadPool);

        //final PollerImpl reactor = new PollerImpl();
        //reactor.start();
        //actorsThreadPool.execute(reactor);

        //ActorThread actorThread = actors.startActorThread();

		/*
	    PVStructure data = PVDataFactory.getPVDataCreate().createPVStructure(simpleStructure);
	    data.getSubField(PVInt.class, "value").put(0x12345678);
*/
	    
	    PVByteArray data = (PVByteArray)PVDataFactory.getPVDataCreate().createPVScalarArray(ScalarType.pvByte);
	    data.setLength(1000*1000*10);
		
	    boolean isRx = false, isTx = false;
	    if (args.length < 2)
	    	isRx = isTx = true;
	    else if (args[1].equals("rx"))
	    	isRx = true;
	    else if (args[1].equals("tx"))
	    	isTx = true;
	    else
	    	throw new IllegalArgumentException("invalid mode");
	    
		final RTPSMessageReceiver rtpsReceiver = isRx ? 
				new RTPSMessageReceiver(args.length > 0 ? args[0] : null, 0, 0x87654321, data.getField()) : null;
		if (isRx)
		{
			final DatagramChannel discoveryMulticastChannel = rtpsReceiver.getDiscoveryMulticastChannel();
		    new Thread(new Runnable() {
		    	public void run() {
		    	    ByteBuffer rxBuffer = ByteBuffer.allocate(64000);
		    		try
		    		{
		    			while (true)
		    			{
		    				rxBuffer.clear();
				    	    SocketAddress receivedFrom = discoveryMulticastChannel.receive(rxBuffer);
				    	    rxBuffer.flip();
				    	    rtpsReceiver.processMessage(receivedFrom, rxBuffer);
		    			}
		    		}
		    		catch (Throwable th) 
		    		{
		    			th.printStackTrace();
		    		}
		    	}
		    }, "rx-multicast-thread").start();
		
		    // TODO
		    /* commented since this will case 2 threads calling rtpsReceiver.processMessage (not thread safe!)
			final DatagramChannel discoveryUnicastChannel = rtpsReceiver.getDiscoveryUnicastChannel();
		    new Thread(new Runnable() {
		    	public void run() {
		    	    ByteBuffer rxBuffer = ByteBuffer.allocate(64000);
		    		try
		    		{
		    			while (true)
		    			{
		    				rxBuffer.clear();
				    	    SocketAddress receivedFrom = discoveryUnicastChannel.receive(rxBuffer);
				    	    rxBuffer.flip();
				    	    rtpsReceiver.processMessage(receivedFrom, rxBuffer);
		    			}
		    		}
		    		catch (Throwable th) 
		    		{
		    			th.printStackTrace();
		    		}
		    	}
		    }, "rx-unicast-thread").start();
			*/
		    Thread.sleep(1000);
		}
		
		
	    final RTPSMessageTransmitter tx = isTx ? 
	    		new RTPSMessageTransmitter(args.length > 0 ? args[0] : null, 0, 0x12345678) : null;

	    
	    if (isTx)
	    {
	
			final DatagramChannel txDiscoveryUnicastChannel = tx.getDiscoveryUnicastChannel();
		    new Thread(new Runnable() {
		    	public void run() {
		    	    ByteBuffer rxBuffer = ByteBuffer.allocate(64000);
		    		try
		    		{
		    			while (true)
		    			{
		    				rxBuffer.clear();
				    	    SocketAddress receivedFrom = txDiscoveryUnicastChannel.receive(rxBuffer);
				    	    rxBuffer.flip();
				    	    tx.processMessage(receivedFrom, rxBuffer);
		    			}
		    		}
		    		catch (Throwable th) 
		    		{
		    			th.printStackTrace();
		    		}
		    	}
		    }, "tx-rx-thread").start();
	
		    
		    
		    
		    
		    new Thread(new Runnable() {
		    	public void run() {
		    		try
		    		{
			    		tx.sendProcess();
		    		}
		    		catch (Throwable th) 
		    		{
		    			th.printStackTrace();
		    		}
		    	}
		    }, "tx-thread").start();
	
		    Thread.sleep(1000);
	    }
	    
	    /*
	    // test
	    tx.takeFreeBuffer();
	    tx.addMessageHeader(tx.getBuffer());
	    tx.addHeartbeatSubmessage(tx.getBuffer(), 1, 2);
	    
	    SequenceNumberSet sns = new SequenceNumberSet();
	    sns.reset(1234);
	    sns.set(1236);
	    sns.set(1237);
	    //sns.set(1234+254);
	    //sns.set(1234+255);
	    tx.addAckNackSubmessage(tx.getBuffer(), sns);
	    
	    tx.sendBuffer();
	    */
	    final long TIMEOUT = 3000;
	    
	    if (isRx && isTx)
	    {
		    long ss = getSerializationSize(data);
	    	byte[] countArray = new byte[1]; byte count = 0;
		    ByteArrayData bad = new ByteArrayData();
		    while(true)
			{
					long t1 = System.currentTimeMillis();
			    	countArray[0] = count++;
			    	data.put(0, 1, countArray, 0);
				    tx.send(data);
				    PVField rdata = rtpsReceiver.waitForNewData(data, TIMEOUT);
					long t2 = System.currentTimeMillis();
		System.out.println(rtpsReceiver.getStatistics());
					double bw = 8*(ss*1000)/(double)(t2-t1)/1000/1000/1000;
					//System.out.println(bw + " Gbit/s");
		    		byte v = 0;
		    		if (rdata != null)
		    		{
		    			((PVByteArray)rdata).get(0, 1, bad);
		    			v = bad.data[0];
		    		}
					System.out.printf("[%d] %.3f %d\n", v, rdata == null ? 0 : bw, rtpsReceiver.getStatistics().missedSN);
					rtpsReceiver.getStatistics().reset();
			}
	    }
	    else if (isRx)
	    {
		    long ss = getSerializationSize(data);
		    ByteArrayData bad = new ByteArrayData();
		    byte prevv = -1; int missedMessage = 0;
		    while(true)
		    {
		    		long t1 = System.currentTimeMillis();

		    		// tx send
		    		
		    	    PVField rdata = rtpsReceiver.waitForNewData(data, TIMEOUT);
		    		long t2 = System.currentTimeMillis();
		   //System.out.println(rtpsReceiver.getStatistics());
		    		double bw = 8*(ss*1000)/(double)(t2-t1)/1000/1000/1000;
		    		//System.out.println(bw + " Gbit/s");
		    		byte v = 0;
		    		if (rdata != null)
		    		{
		    			((PVByteArray)rdata).get(0, 1, bad);
		    			v = bad.data[0];
		    			if ((byte)(prevv + 1) != v)
		    				missedMessage++;
		    			prevv = v;
		    		}
		    		System.out.printf("[%d] %.3f %d, missed messages: %d\n", v, rdata == null ? 0 : bw, rtpsReceiver.getStatistics().missedSN, missedMessage);
		    		rtpsReceiver.getStatistics().reset();
		    }
	    	
	    }
	    else if (isTx)
	    {
	    	//long sleepTime = Long.valueOf(System.getProperty("DELAY", "0"));
	    	int clients = Integer.valueOf(System.getProperty("CLIENTS", "1"));
	    	byte[] countArray = new byte[1]; byte count = 0;
		    while(true)
		    {
		    	countArray[0] = count++;
		    	data.put(0, 1, countArray, 0);

		    	tx.send(data);
		    	tx.waitUntilReceived(clients, TIMEOUT);		// TODO
		    	
		    	//// 0.1GB/sec, 10MB...1/10s
		    	//if (sleepTime > 0)
		    	//	Thread.sleep(sleepTime);
		    }
	    }
	    
	    
	    System.out.println("done.");
	    
	    /*
	    
	    long startTime = System.nanoTime();
	    
	    long timeSpent = System.nanoTime() - startTime;
	    System.out.println(" done in " + timeSpent/1000/1000 + "ms");
	    */
	    
	}

}



// TODO recovery resendProcess and thready safety!!!!!
// TODO send ackNack more often (before)

// TODO periodic send of ackNack (or heartbeat with final)
