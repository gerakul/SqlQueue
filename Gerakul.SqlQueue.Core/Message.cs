using System;
using System.Collections.Generic;
using System.Text;

namespace Gerakul.SqlQueue.Core
{
    public class Message
    {
        public long ID { get; private set; }
        public DateTime Created { get; private set; }
        public byte[] Body { get; private set; }

        public Message(long id, DateTime created, byte[] body)
        {
            this.ID = id;
            this.Created = created;
            this.Body = body;
        }

        public static readonly Message Empty = new Message(0, default(DateTime), null);
    }
}
