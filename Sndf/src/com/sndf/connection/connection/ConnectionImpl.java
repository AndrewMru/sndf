package com.sndf.connection.connection;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import com.sndf.connection.decode.DefaultMessageDecoderFactory;
import com.sndf.connection.decode.IStreamDecoderEx;
import com.sndf.connection.decode.IStreamDecoderExFactory;
import com.sndf.connection.encode.DefaultMessageEncoderFactory;
import com.sndf.connection.encode.IStreamEncoder;
import com.sndf.connection.encode.IStreamEncoderFactory;
import com.sndf.connection.message.IMessage;

/**
 * Represents a tcp connection between a Client and a Server.
 */
public class ConnectionImpl implements IConnection
{
    private static final Object sIdLock = new Object();
    private static final int INVALID_ID = -1;
    
    private static int generationId = 0;
    private int mConnectionId = INVALID_ID;

    private final IStreamDecoderExFactory<IMessage> mDecoderFactory;
    private IStreamDecoderEx<IMessage> mMessageDecoder;

    private final IStreamEncoderFactory<IMessage> mEncoderFactory;
    private IStreamEncoder<IMessage> mMessageEncoder;

    private SocketChannel mSocketChannel;
    private SelectionKey mSelectionKey;
    
    private ByteBuffer mReadBuffer;
    private ByteBuffer mWriteBuffer;
    
    private Object mWriteLock = new Object();

    private boolean mIsConnected = false;

    public ConnectionImpl(IStreamDecoderExFactory<IMessage> decoderFactory, IStreamEncoderFactory<IMessage> encoderFactory)
    {
    	generateId();
    	
        mReadBuffer = ByteBuffer.allocate(1024);
        mWriteBuffer = ByteBuffer.allocate(1024);
        
        mReadBuffer.clear();
        mWriteBuffer.clear();

        mDecoderFactory = decoderFactory;
        mEncoderFactory = encoderFactory;
    }
    
    public ConnectionImpl()
    {
        this(new DefaultMessageDecoderFactory(), new DefaultMessageEncoderFactory());
    }
    
    private void generateId() 
	{
		synchronized (sIdLock) 
		{
			mConnectionId = generationId++;
		}
	}	

    @Override
    public int getId()
    {
        return mConnectionId;
    }

    @Override
    public boolean isConnected()
    {
        return mIsConnected;
    }

    /**
     * Connects this socket to the given remote host address and port specified
     * by the SocketAddress {@code remoteAddr} with the specified timeout. The
     * connecting method will block until the connection is established or an
     * error occurred.
     */
    @Override
    public void connect(Selector selector, SocketAddress remoteAddress, int timeout) throws IOException
    {
        close();

        try
        {
            SocketChannel socketChannel = selector.provider().openSocketChannel();
            Socket socket = socketChannel.socket();
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            socketChannel.socket().connect(remoteAddress, timeout);
            socketChannel.configureBlocking(false);

            mSelectionKey = socketChannel.register(selector, SelectionKey.OP_READ);
            mSelectionKey.attach(this);
            mSocketChannel = socketChannel;

            mMessageDecoder = mDecoderFactory.createStreamDecoder(); 
            mMessageEncoder = mEncoderFactory.createSteamEncoder();

            mIsConnected = true;
        }
        catch (IOException e)
        {
            close();
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Accepts a connection to this server-socket channel.
     * This method just set some attributes and register OP_READ option of the new 
     * socketchannel to the selector.
     */
    @Override
    public void onAccepted(Selector selector, SocketChannel socketChannel) throws IOException
    {
        mSocketChannel = socketChannel;
        mMessageDecoder = mDecoderFactory.createStreamDecoder(); 
        mMessageEncoder = mEncoderFactory.createSteamEncoder();

        try
        {
            socketChannel.configureBlocking(false);
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setKeepAlive(true);

            mSelectionKey = socketChannel.register(selector, SelectionKey.OP_READ);
            mSelectionKey.attach(this);
            mIsConnected = true;
        }
        catch (IOException e)
        {
            close();
            throw e;
        }
    }

    /**
     * Closes the connection. It is not possible to reconnect or rebind to this
     * socket thereafter which means a new socket instance has to be created.
     * 
     * @throws IOException
     *             if an error occurs while closing the socket.
     */
    @Override
    public void close()
    {
        if (null != mSocketChannel)
        {
            try
            {
                mIsConnected = false;
                mSocketChannel.close();
                mSocketChannel = null;

                if (null != mSelectionKey)
                {
                    mSelectionKey.selector().wakeup();
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
    }

    /**
     * Reads message from this socket channel.
     * @return the message actually read.
     */
    @Override
    public List<IMessage> readMessage() throws IOException
    {
        SocketChannel socketChannel = mSocketChannel;
        List<IMessage> result = null;

        while(true)
        {
            mReadBuffer.clear();
            int bytesRead = socketChannel.read(mReadBuffer);

            if (bytesRead < 0)
            {
                close();
                throw new IOException("the connection is closed");
            }
            else if (bytesRead > 0)
            {
                mReadBuffer.flip();
                List<IMessage> msgList = mMessageDecoder.decode(mReadBuffer.array(), 0, mReadBuffer.remaining());

                if (null == msgList || msgList.size() <= 0)
                {
                    break;
                }

                if (null == result)
                {
                    result = new ArrayList<IMessage>();
                }
                result.addAll(msgList);
            }
            else 
            {
                break;
            }
        }
        
        return result;
    }
    
    /**
     * Writes message to this socket channel. The
     * The call may block if other threads are also attempting to write to the
     * same channel.
     * @param msg the message to be written.
    */
    @Override
    public void sendMessage(IMessage msg)
    {
    	SocketChannel socketChannel = mSocketChannel;
    	
        if (null == socketChannel && !mIsConnected)
        {
            return;
        }

        byte[] bytes = mMessageEncoder.encode(msg);
        
        if (null == bytes || 0 == bytes.length)
        {
        	return ;
        }
        
        ByteBuffer writeBuffer = mWriteBuffer;
        writeBuffer.put(bytes);

        try
        {
        	if (!writeToSocket())
        	{
        		mSelectionKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        	}
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
    
    @Override
    public void write() throws IOException 
    {
    	synchronized (mWriteLock) 
    	{
    		if (writeToSocket())
    		{
    			mSelectionKey.interestOps(SelectionKey.OP_READ);
    		}
		}
    }
    
    private boolean writeToSocket() throws IOException
    {
    	SocketChannel socketChannel = mSocketChannel;
    	
        if (null == socketChannel && !mIsConnected)
        {
        	throw new RuntimeException("Connection is closed");
        }
        
        ByteBuffer buffer = mWriteBuffer;
        buffer.flip();
        
        while(buffer.hasRemaining())
        {
        	if (0 == socketChannel.write(buffer))
        	{
        		break;
        	}
        }
        
        buffer.compact();
        return 0 == buffer.position();
    }
}
