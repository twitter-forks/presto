/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.twitter.hive.thrift;

import io.airlift.log.Logger;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DummyClass implements TBase<DummyClass, DummyClass.Fields>
{
    private static final Logger log = Logger.get(DummyClass.class);
    public final Map<Short, Object> values = new HashMap<>();

    public DummyClass() {}

    public class Fields implements TFieldIdEnum
    {
        private final short thriftId;
        private final String fieldName;

        Fields(short thriftId, String fieldName)
        {
            this.thriftId = thriftId;
            this.fieldName = fieldName;
        }

        public short getThriftFieldId()
        {
            return thriftId;
        }

        public String getFieldName()
        {
            return fieldName;
        }
    }

    public DummyClass deepCopy()
    {
        return new DummyClass();
    }

    public void clear() {}

    public Fields fieldForId(int fieldId)
    {
        return new Fields((short) fieldId, "dummy");
    }

    public Object getFieldValue(Fields field)
    {
        return values.get(field.thriftId);
    }

    public boolean isSet(Fields field)
    {
        return values.containsKey(field.getThriftFieldId());
    }

    public void read(TProtocol iprot) throws TException
    {
        TField field;
        // int i = 0;
        iprot.readStructBegin();
        while (true) {
            field = iprot.readFieldBegin();
            if (field.type == TType.STOP) {
                break;
            }
            // NOTES: The thrift id may not the same of hive table column number
            // We can either fill the hive table with discontinued id or discard
            // the field.id here and use a counter as its column number. Like:
            // values.put((short) ++i, readElem(iprot, field.type));
            values.put((short) (field.id - 1), readElem(iprot, field.type));
            iprot.readFieldEnd();
        }
        iprot.readStructEnd();
    }

    private Object readElem(TProtocol iprot, byte type) throws TException
    {
        switch (type) {
            case TType.BOOL:
                return iprot.readBool();
            case TType.BYTE:
                return iprot.readByte();
            case TType.I16:
                return iprot.readI16();
            case TType.ENUM:
            case TType.I32:
                return iprot.readI32();
            case TType.I64:
                return iprot.readI64();
            case TType.DOUBLE:
                return iprot.readDouble();
            case TType.STRING:
                return iprot.readString();
            case TType.STRUCT:
                return readStruct(iprot);
            case TType.LIST:
                return readList(iprot);
            case TType.SET:
                return readSet(iprot);
            case TType.MAP:
                return readMap(iprot);
            default:
                TProtocolUtil.skip(iprot, type);
        }
        return null;
    }

    private Object readStruct(TProtocol iprot) throws TException
    {
        DummyClass elem = new DummyClass();
        elem.read(iprot);
        return elem;
    }

    private Object readList(TProtocol iprot) throws TException
    {
        TList ilist = iprot.readListBegin();
        List<Object> listValue = new ArrayList<>();
        for (int i = 0; i < ilist.size; ++i) {
            listValue.add(readElem(iprot, ilist.elemType));
        }
        iprot.readListEnd();
        return listValue;
    }

    private Object readSet(TProtocol iprot) throws TException
    {
        TSet iset = iprot.readSetBegin();
        List<Object> setValue = new ArrayList<>();
        for (int i = 0; i < iset.size; ++i) {
            setValue.add(readElem(iprot, iset.elemType));
        }
        iprot.readSetEnd();
        return setValue;
    }

    private Object readMap(TProtocol iprot) throws TException
    {
        TMap imap = iprot.readMapBegin();
        Map<Object, Object> mapValue = new HashMap<>();
        for (int i = 0; i < imap.size; ++i) {
            mapValue.put(readElem(iprot, imap.keyType), readElem(iprot, imap.valueType));
        }
        iprot.readMapEnd();
        return mapValue;
    }

    public void setFieldValue(Fields field, Object value)
    {
        throw new UnsupportedOperationException("DummyClass.setFieldValue is not supported.");
    }

    public void write(TProtocol oprot)
    {
        throw new UnsupportedOperationException("DummyClass.write is not supported.");
    }

    public int compareTo(DummyClass other)
    {
        throw new UnsupportedOperationException("Dummy.compareTo is not supported.");
    }
}
