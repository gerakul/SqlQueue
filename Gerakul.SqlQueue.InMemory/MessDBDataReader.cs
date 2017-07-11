using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Gerakul.SqlQueue.InMemory
{
    internal class MessDbDataReader : DbDataReader
    {
        IEnumerator<byte[]> enumerator;
        int id = 0;
        byte[] body;

        public MessDbDataReader(IEnumerable<byte[]> data)
        {
            enumerator = data.GetEnumerator();
        }

        public override object this[int ordinal] => ordinal == 0 ? (object)id : (object)body;

        public override object this[string name] => name == "ID" ? (object)id : (object)body;

        public override int Depth => 0;

        public override int FieldCount => 2;

        public override bool HasRows => throw new NotImplementedException();

        public override bool IsClosed => false;

        public override int RecordsAffected => throw new NotImplementedException();

        public override bool GetBoolean(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override byte GetByte(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            byte[] bytes = body;

            if (dataOffset < 0 || dataOffset >= bytes.Length || bufferOffset < 0 || bufferOffset >= buffer.Length)
            {
                return 0;
            }

            int fo = checked((int)dataOffset);

            int len = Math.Min(Math.Min(bytes.Length - fo, buffer.Length - bufferOffset), length);
            Array.Copy(bytes, fo, buffer, bufferOffset, len);

            return len;
        }

        public override char GetChar(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException();
        }

        public override string GetDataTypeName(int ordinal)
        {
            return ordinal == 0 ? typeof(int).Name : typeof(byte[]).Name;
        }

        public override DateTime GetDateTime(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override decimal GetDecimal(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override double GetDouble(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override IEnumerator GetEnumerator()
        {
            return enumerator;
        }

        public override Type GetFieldType(int ordinal)
        {
            return ordinal == 0 ? typeof(int) : typeof(byte[]);
        }

        public override float GetFloat(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override Guid GetGuid(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override short GetInt16(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override int GetInt32(int ordinal)
        {
            return id;
        }

        public override long GetInt64(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override string GetName(int ordinal)
        {
            return ordinal == 0 ? "ID" : "Body";
        }

        public override int GetOrdinal(string name)
        {
            return name == "ID" ? 0 : 1;
        }

        public override string GetString(int ordinal)
        {
            throw new NotImplementedException();
        }

        public override object GetValue(int ordinal)
        {
            return ordinal == 0 ? (object)id : (object)body;
        }

        public override int GetValues(object[] values)
        {
            values[0] = id;
            values[1] = body;
            return 2;
        }

        public override bool IsDBNull(int ordinal)
        {
            return ordinal == 0 ? false : (body == null);
        }

        public override bool NextResult()
        {
            throw new NotImplementedException();
        }

        public override bool Read()
        {
            if (enumerator.MoveNext())
            {
                id++;
                body = enumerator.Current;
                return true;
            }

            return false;
        }
    }
}
