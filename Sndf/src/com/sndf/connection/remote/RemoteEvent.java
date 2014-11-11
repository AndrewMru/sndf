package com.sndf.connection.remote;

import java.util.HashMap;
import java.util.Map;

import com.sndf.connection.message.IMessage;

public class RemoteEvent extends IMessage
{
    private static final long serialVersionUID = -5043866380833058448L;
    
    private static final int INVALID_VALUE = -1;
    
    /**
     * The type of remote event, the define of type see {@link RemoteEventType} 
     */
    private int mType = INVALID_VALUE;
    
    /**
     * The params of remote event, the define of key see {@link RemoteEventParams} 
     */
    private final Map<Integer, Object> mParamsMap;
    
    public RemoteEvent(int type)
    {
        this.mType = type;
        mParamsMap = new HashMap<Integer, Object>();
    }
    
    public int getType()
    {
        return mType;
    }

    public RemoteEvent setType(int type)
    {
        this.mType = type;
        return this;
    }
    
    public Object getParams(int key)
    {
        return mParamsMap.get(key);
    }
    
    public RemoteEvent putParams(int key, Object value)
    {
        mParamsMap.put(key, value);
        return this;
    }
    
    /**
     * Clear all the data of remote event.
     * @return the empty remote event.
     */
    public RemoteEvent clear()
    {
        mType = INVALID_VALUE;
        mParamsMap.clear();
        return this;
    }
}
