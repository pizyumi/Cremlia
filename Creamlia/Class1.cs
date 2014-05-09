using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Creamlia
{
    public interface ICremliaIdFactory
    {
        ICremliaId Create();
    }

    public interface ICremliaId
    {
        int Size { get; }
        byte[] Bytes { get; }

        void FromBytes(byte[] bytes);
        ICremliaId XOR(ICremliaId id);
    }

    public interface ICremliaNodeInfomation
    {
        ICremliaId Id { get; }
    }

    public interface ICremliaNetworkIo
    {
        event EventHandler<ICremliaNetworkIoSession> SessionStarted;

        ICremliaNetworkIoSession StartSession(ICremliaNodeInfomation nodeInfo);
    }

    public interface ICremliaNetworkIoSession
    {
        ICremliaNodeInfomation NodeInfo { get; }

        void Write(CremliaMessageBase message);
        CremliaMessageBase Read();
        void Close();
    }

    public interface ICremliaDatabaseIo
    {
        int TExpire { set; }

        byte[] Get(ICremliaId id);
        Tuple<ICremliaId, byte[]>[] GetOriginals();
        Tuple<ICremliaId, byte[]>[] GetCharges();
        void Set(ICremliaId id, byte[] data, bool isOriginal, bool isCache);
    }

    public class Cremlia
    {
        public Cremlia(ICremliaIdFactory _idFactory, ICremliaDatabaseIo _databaseIo, ICremliaNetworkIo _networkIo, ICremliaNodeInfomation _myNodeInfo) : this(_idFactory, _databaseIo, _networkIo, _myNodeInfo, 256, 20, 3, 86400 * 1000, 3600 * 1000, 3600 * 1000, 86400 * 1000) { }

        public Cremlia(ICremliaIdFactory _idFactory, ICremliaDatabaseIo _databaseIo, ICremliaNetworkIo _networkIo, ICremliaNodeInfomation _myNodeInfo, int _keySpace, int _K, int _α, int _tExpire, int _tRefresh, int _tReplicate, int _tRepublish)
        {
            //鍵空間は8の倍数とする
            if (_keySpace % 8 != 0)
                throw new InvalidDataException("key_space");
            if (_myNodeInfo.Id.Size != _keySpace)
                throw new InvalidDataException("id_size_and_key_space");

            idFactory = _idFactory;
            databaseIo = _databaseIo;
            networkIo = _networkIo;
            myNodeInfo = _myNodeInfo;
            keySpace = _keySpace;
            K = _K;
            α = _α;
            tExpire = _tExpire;
            tRefresh = _tRefresh;
            tReplicate = _tReplicate;
            tRepublish = _tRepublish;

            kbuckets = new List<ICremliaNodeInfomation>[keySpace];
            for (int i = 0; i < kbuckets.Length; i++)
                kbuckets[i] = new List<ICremliaNodeInfomation>();
            kbucketsLocks = new object[keySpace];
            for (int i = 0; i < kbucketsLocks.Length; i++)
                kbucketsLocks[i] = new object();
            kbucketsUpdatedTime = new DateTime[keySpace];

            networkIo.SessionStarted += (sender, e) =>
            {
                Action<ICremliaId> _ResNeighborNodes = (id) =>
                {
                    SortedList<ICremliaId, ICremliaNodeInfomation> findTable = new SortedList<ICremliaId, ICremliaNodeInfomation>();
                    GetNeighborNodesTable(id, findTable);
                    e.Write(new NeighborNodesMessage(findTable.Values.ToArray()));
                };

                CremliaMessageBase message = e.Read();
                if (message is PingReqMessage)
                    e.Write(new PingResMessage());
                else if (message is StoreReqMessage)
                {
                    StoreReqMessage srm = message as StoreReqMessage;
                    databaseIo.Set(srm.id, srm.data, false, false);
                }
                else if (message is FindNodesReqMessage)
                    _ResNeighborNodes((message as FindNodesReqMessage).id);
                else if (message is FindValueReqMessage)
                {
                    ICremliaId id = (message as FindValueReqMessage).id;

                    byte[] data = databaseIo.Get(id);
                    if (data == null)
                        _ResNeighborNodes(id);
                    else
                        e.Write(new ValueMessage(data));
                }
                //独自実装
                else if (message is GetIdsAndValuesReqMessage)
                    e.Write(new IdsAndValuesMessage(databaseIo.GetCharges()));
                else
                    throw new NotSupportedException("cremlia_not_supported_message");

                e.Close();

                UpdateNodeState(e.NodeInfo, true);
            };

            databaseIo.TExpire = tExpire;

            Timer timerRefresh = new Timer((state) =>
            {
                for (int i = 0; i < kbuckets.Length; i++)
                    //時間が掛かるので若干判定条件を短めに
                    if (kbuckets[i].Count != 0 && kbucketsUpdatedTime[i] <= DateTime.Now - new TimeSpan(0, 0, (int)(tRefresh * 0.9)))
                        FindNodes(GetRamdomHash(i));
            }, null, tRefresh, tRefresh);

            Action<Tuple<ICremliaId, byte[]>[]> _StoreIdsAndValues = (idsAndValues) =>
            {
                foreach (Tuple<ICremliaId, byte[]> idAndValue in idsAndValues)
                    foreach (ICremliaNodeInfomation nodeInfo in FindNodes(idAndValue.Item1))
                        ReqStore(nodeInfo, idAndValue.Item1, idAndValue.Item2);
            };

            Timer timerReplicate = new Timer((state) => _StoreIdsAndValues(databaseIo.GetCharges()), null, tReplicate, tReplicate);
            Timer timerRepublish = new Timer((state) => _StoreIdsAndValues(databaseIo.GetOriginals()), null, tRepublish, tRepublish);
        }

        private readonly ICremliaIdFactory idFactory;
        private readonly ICremliaDatabaseIo databaseIo;
        private readonly ICremliaNetworkIo networkIo;
        private readonly object[] kbucketsLocks;
        private readonly List<ICremliaNodeInfomation>[] kbuckets;
        private readonly DateTime[] kbucketsUpdatedTime;

        public readonly ICremliaNodeInfomation myNodeInfo;
        public readonly int keySpace;
        public readonly int K;
        public readonly int α;
        public readonly int tExpire;
        public readonly int tRefresh;
        public readonly int tReplicate;
        public readonly int tRepublish;

        public bool ReqPing(ICremliaNodeInfomation nodeInfo)
        {
            if (nodeInfo.Equals(myNodeInfo))
                throw new ArgumentException("cremlia_my_node");

            ICremliaNetworkIoSession session = networkIo.StartSession(nodeInfo);
            if (session == null)
            {
                UpdateNodeState(nodeInfo, false);
                return false;
            }

            session.Write(new PingReqMessage());
            CremliaMessageBase message = session.Read();
            session.Close();

            if (message == null || !(message is PingResMessage))
            {
                UpdateNodeState(nodeInfo, false);
                return false;
            }

            UpdateNodeState(nodeInfo, true);
            return true;
        }

        public bool ReqStore(ICremliaNodeInfomation nodeInfo, ICremliaId id, byte[] data)
        {
            if (nodeInfo.Equals(myNodeInfo))
                throw new ArgumentException("cremlia_my_node");

            ICremliaNetworkIoSession session = networkIo.StartSession(nodeInfo);
            if (session == null)
            {
                UpdateNodeState(nodeInfo, false);
                return false;
            }

            session.Write(new StoreReqMessage(id, data));
            session.Close();

            UpdateNodeState(nodeInfo, true);
            return true;
        }

        public ICremliaNodeInfomation[] ReqFindNodes(ICremliaNodeInfomation nodeInfo, ICremliaId id)
        {
            if (nodeInfo.Equals(myNodeInfo))
                throw new ArgumentException("cremlia_my_node");

            ICremliaNetworkIoSession session = networkIo.StartSession(nodeInfo);
            if (session == null)
            {
                UpdateNodeState(nodeInfo, false);
                return null;
            }

            session.Write(new FindNodesReqMessage(id));
            CremliaMessageBase message = session.Read();
            session.Close();

            if (message == null || !(message is NeighborNodesMessage))
            {
                UpdateNodeState(nodeInfo, false);
                return null;
            }

            UpdateNodeState(nodeInfo, true);
            return (message as NeighborNodesMessage).nodeInfos;
        }

        public MultipleReturn<ICremliaNodeInfomation[], byte[]> ReqFindValue(ICremliaNodeInfomation nodeInfo, ICremliaId id)
        {
            if (nodeInfo.Equals(myNodeInfo))
                throw new ArgumentException("cremlia_my_node");

            ICremliaNetworkIoSession session = networkIo.StartSession(nodeInfo);
            if (session == null)
            {
                UpdateNodeState(nodeInfo, false);
                return null;
            }

            session.Write(new FindValueReqMessage(id));
            CremliaMessageBase message = session.Read();
            session.Close();

            if (message != null)
                if (message is NeighborNodesMessage)
                {
                    UpdateNodeState(nodeInfo, true);
                    return new MultipleReturn<ICremliaNodeInfomation[], byte[]>((message as NeighborNodesMessage).nodeInfos);
                }
                else if (message is ValueMessage)
                {
                    UpdateNodeState(nodeInfo, true);
                    return new MultipleReturn<ICremliaNodeInfomation[], byte[]>((message as ValueMessage).data);
                }

            UpdateNodeState(nodeInfo, false);
            return null;
        }

        //独自実装
        public Tuple<ICremliaId, byte[]>[] ReqGetIdsAndValues(ICremliaNodeInfomation nodeInfo)
        {
            if (nodeInfo.Equals(myNodeInfo))
                throw new ArgumentException("cremlia_my_node");

            ICremliaNetworkIoSession session = networkIo.StartSession(nodeInfo);
            if (session == null)
            {
                UpdateNodeState(nodeInfo, false);
                return null;
            }

            session.Write(new GetIdsAndValuesReqMessage());
            CremliaMessageBase message = session.Read();
            session.Close();

            if (message == null || !(message is IdsAndValuesMessage))
            {
                UpdateNodeState(nodeInfo, false);
                return null;
            }

            UpdateNodeState(nodeInfo, true);
            return (message as IdsAndValuesMessage).idsAndValues;
        }

        public ICremliaNodeInfomation[] FindNodes(ICremliaId id)
        {
            object findLock = new object();
            SortedList<ICremliaId, ICremliaNodeInfomation> findTable = new SortedList<ICremliaId, ICremliaNodeInfomation>();
            Dictionary<ICremliaNodeInfomation, bool> checkTable = new Dictionary<ICremliaNodeInfomation, bool>();
            Dictionary<ICremliaNodeInfomation, bool?> checkTableSucceedOrFail = new Dictionary<ICremliaNodeInfomation, bool?>();

            GetNeighborNodesTable(id, findTable);
            foreach (var nodeInfo in findTable.Values)
                checkTable.Add(nodeInfo, false);
            foreach (var nodeInfo in findTable.Values)
                checkTableSucceedOrFail.Add(nodeInfo, null);

            AutoResetEvent[] ares = new AutoResetEvent[α];
            for (int i = 0; i < α; i++)
            {
                int inner = i;

                ares[inner] = new AutoResetEvent(false);

                this.StartTask("cremlia_find_nodes", "cremlia_find_nodes", () =>
                {
                    while (true)
                    {
                        ICremliaNodeInfomation nodeInfo = null;
                        int c = 0;
                        lock (findLock)
                            for (int j = 0; j < findTable.Count; j++)
                                if (!checkTable[findTable[findTable.Keys[j]]])
                                {
                                    nodeInfo = findTable[findTable.Keys[j]];
                                    checkTable[findTable[findTable.Keys[j]]] = true;
                                    break;
                                }
                                else if (checkTableSucceedOrFail[findTable[findTable.Keys[j]]] == true && ++c == K)
                                    break;

                        //適切なノード情報が見付からない場合はほぼ確実にノードの探索は殆ど終わっていると想定し、残りの処理は他のスレッドに任せる
                        if (nodeInfo == null)
                            break;
                        else
                        {
                            ICremliaNodeInfomation[] nodeInfos = ReqFindNodes(nodeInfo, id);
                            lock (findLock)
                                if (nodeInfos == null)
                                    checkTableSucceedOrFail[nodeInfo] = false;
                                else
                                {
                                    checkTableSucceedOrFail[nodeInfo] = true;

                                    foreach (ICremliaNodeInfomation ni in nodeInfos)
                                    {
                                        ICremliaId xor = id.XOR(ni.Id);
                                        if (!ni.Equals(myNodeInfo) && !findTable.Keys.Contains(xor))
                                        {
                                            findTable.Add(xor, ni);
                                            checkTable.Add(ni, false);
                                            checkTableSucceedOrFail.Add(ni, null);
                                        }
                                    }
                                }
                        }
                    }

                    ares[inner].Set();
                });
            }

            for (int i = 0; i < α; i++)
                ares[i].WaitOne();

            List<ICremliaNodeInfomation> findNodes = new List<ICremliaNodeInfomation>();
            for (int i = 0; i < findTable.Count && findNodes.Count < K; i++)
                if (checkTable[findTable[findTable.Keys[i]]] && checkTableSucceedOrFail[findTable[findTable.Keys[i]]] == true)
                    findNodes.Add(findTable[findTable.Keys[i]]);

            return findNodes.ToArray();
        }

        public byte[] FindValue(ICremliaId id)
        {
            object findLock = new object();
            SortedList<ICremliaId, ICremliaNodeInfomation> findTable = new SortedList<ICremliaId, ICremliaNodeInfomation>();
            Dictionary<ICremliaNodeInfomation, bool> checkTable = new Dictionary<ICremliaNodeInfomation, bool>();
            Dictionary<ICremliaNodeInfomation, bool?> checkTableSucceedOrFail = new Dictionary<ICremliaNodeInfomation, bool?>();

            GetNeighborNodesTable(id, findTable);
            foreach (var nodeInfo in findTable.Values)
                checkTable.Add(nodeInfo, false);
            foreach (var nodeInfo in findTable.Values)
                checkTableSucceedOrFail.Add(nodeInfo, null);

            byte[] data = null;
            AutoResetEvent[] ares = new AutoResetEvent[α];
            for (int i = 0; i < α; i++)
            {
                int inner = i;

                ares[inner] = new AutoResetEvent(false);

                this.StartTask("cremlia_find_value", "cremlia_find_value", () =>
                {
                    while (data == null)
                    {
                        ICremliaNodeInfomation nodeInfo = null;
                        int c = 0;
                        lock (findLock)
                            for (int j = 0; j < findTable.Count; j++)
                                if (!checkTable[findTable[findTable.Keys[j]]])
                                {
                                    nodeInfo = findTable[findTable.Keys[j]];
                                    checkTable[findTable[findTable.Keys[j]]] = true;
                                    break;
                                }
                                else if (checkTableSucceedOrFail[findTable[findTable.Keys[j]]] == true && ++c == K)
                                    break;

                        //適切なノード情報が見付からない場合はほぼ確実にノードの探索は殆ど終わっていると想定し、残りの処理は他のスレッドに任せる
                        if (nodeInfo == null)
                            break;
                        else
                        {
                            MultipleReturn<ICremliaNodeInfomation[], byte[]> multipleReturn = ReqFindValue(nodeInfo, id);
                            lock (findLock)
                                if (multipleReturn == null)
                                    checkTableSucceedOrFail[nodeInfo] = false;
                                else
                                {
                                    checkTableSucceedOrFail[nodeInfo] = true;

                                    if (multipleReturn.IsValue1)
                                        foreach (ICremliaNodeInfomation ni in multipleReturn.Value1)
                                        {
                                            ICremliaId xor = id.XOR(ni.Id);
                                            if (!ni.Equals(myNodeInfo) && !findTable.Keys.Contains(xor))
                                            {
                                                findTable.Add(xor, ni);
                                                checkTable.Add(ni, false);
                                                checkTableSucceedOrFail.Add(ni, null);
                                            }
                                        }
                                    else if (multipleReturn.IsValue2)
                                        data = multipleReturn.Value2;
                                }
                        }
                    }

                    ares[inner].Set();
                });
            }

            for (int i = 0; i < α; i++)
                ares[i].WaitOne();

            if (data != null)
                databaseIo.Set(id, data, false, true);

            return data;
        }

        public void StoreOriginal(ICremliaId id, byte[] data)
        {
            databaseIo.Set(id, data, true, false);

            foreach (ICremliaNodeInfomation nodeInfo in FindNodes(id))
                ReqStore(nodeInfo, id, data);
        }

        public void Join(ICremliaNodeInfomation[] nodeInfos)
        {
            foreach (ICremliaNodeInfomation nodeInfo in nodeInfos)
                UpdateNodeStateWhenJoin(nodeInfo);

            foreach (ICremliaNodeInfomation nodeInfo in FindNodes(myNodeInfo.Id))
                foreach (Tuple<ICremliaId, byte[]> idAndValue in ReqGetIdsAndValues(nodeInfo))
                    databaseIo.Set(idAndValue.Item1, idAndValue.Item2, false, false);
        }

        public ICremliaId GetRamdomHash(int distanceLevel)
        {
            byte[] bytes = new byte[keySpace / 8];
            for (int i = bytes.Length - 1; i >= 0; i--, distanceLevel -= 8)
                if (distanceLevel >= 8)
                    bytes[i] = (byte)256.RandomNum();
                else
                {
                    if (distanceLevel == 0)
                        bytes[i] = 1;
                    else if (distanceLevel == 1)
                        bytes[i] = (byte)(2 + 2.RandomNum());
                    else if (distanceLevel == 2)
                        bytes[i] = (byte)(4 + 4.RandomNum());
                    else if (distanceLevel == 3)
                        bytes[i] = (byte)(8 + 8.RandomNum());
                    else if (distanceLevel == 4)
                        bytes[i] = (byte)(16 + 16.RandomNum());
                    else if (distanceLevel == 5)
                        bytes[i] = (byte)(32 + 32.RandomNum());
                    else if (distanceLevel == 6)
                        bytes[i] = (byte)(64 + 64.RandomNum());
                    else if (distanceLevel == 7)
                        bytes[i] = (byte)(128 + 128.RandomNum());

                    break;
                }

            ICremliaId xor = idFactory.Create();
            if (xor.Size != keySpace)
                throw new InvalidDataException("invalid_id_size");
            xor.FromBytes(bytes);
            return xor.XOR(myNodeInfo.Id);
        }

        public int GetDistanceLevel(ICremliaId id)
        {
            ICremliaId xor = id.XOR(myNodeInfo.Id);

            //距離が0の場合にはdistanceLevelは-1
            //　　論文ではハッシュ値の衝突が考慮されていないっぽい？
            int distanceLevel = keySpace - 1;
            for (int i = 0; i < xor.Bytes.Length; i++)
                if (xor.Bytes[i] == 0)
                    distanceLevel -= 8;
                else
                {
                    if (xor.Bytes[i] == 1)
                        distanceLevel -= 7;
                    else if (xor.Bytes[i] <= 3 && xor.Bytes[i] >= 2)
                        distanceLevel -= 6;
                    else if (xor.Bytes[i] <= 7 && xor.Bytes[i] >= 4)
                        distanceLevel -= 5;
                    else if (xor.Bytes[i] <= 15 && xor.Bytes[i] >= 8)
                        distanceLevel -= 4;
                    else if (xor.Bytes[i] <= 31 && xor.Bytes[i] >= 16)
                        distanceLevel -= 3;
                    else if (xor.Bytes[i] <= 63 && xor.Bytes[i] >= 32)
                        distanceLevel -= 2;
                    else if (xor.Bytes[i] <= 127 && xor.Bytes[i] >= 64)
                        distanceLevel -= 1;

                    break;
                }

            return distanceLevel;
        }

        public void GetNeighborNodesTable(ICremliaId id, SortedList<ICremliaId, ICremliaNodeInfomation> findTable)
        {
            Func<ICremliaNodeInfomation, bool> TryFindTableAddition = (nodeInfo) =>
            {
                ICremliaId xor = id.XOR(nodeInfo.Id);
                if (!findTable.ContainsKey(xor))
                    findTable.Add(xor, nodeInfo);
                else
                    this.RaiseError("find_table_already_added".GetLogMessage(xor.ToString(), findTable[xor].Id.ToString(), nodeInfo.Id.ToString()), 5);

                return findTable.Count >= K;
            };

            int distanceLevel = GetDistanceLevel(id);

            //原論文では探索するidが自己のノード情報のidと同一である場合を想定していない・・・
            if (distanceLevel == -1)
                distanceLevel = 0;

            lock (kbucketsLocks[distanceLevel])
                foreach (ICremliaNodeInfomation nodeInfo in kbuckets[distanceLevel])
                    if (TryFindTableAddition(nodeInfo))
                        return;

            for (int i = distanceLevel - 1; i >= 0; i--)
                lock (kbucketsLocks[i])
                    foreach (ICremliaNodeInfomation nodeInfo in kbuckets[i])
                        if (TryFindTableAddition(nodeInfo))
                            return;

            for (int i = distanceLevel + 1; i < kbuckets.Length; i++)
                lock (kbucketsLocks[i])
                    foreach (ICremliaNodeInfomation nodeInfo in kbuckets[i])
                        if (TryFindTableAddition(nodeInfo))
                            return;
        }

        public ICremliaNodeInfomation[] GetKbuckets(int distanceLevel)
        {
            List<ICremliaNodeInfomation> nodeInfos = new List<ICremliaNodeInfomation>();
            lock (kbuckets[distanceLevel])
                for (int i = 0; i < kbuckets[distanceLevel].Count; i++)
                    nodeInfos.Add(kbuckets[distanceLevel][i]);
            return nodeInfos.ToArray();
        }

        public void UpdateNodeState(ICremliaNodeInfomation nodeInfo, bool isValid)
        {
            if (nodeInfo.Id.Size != keySpace)
                throw new InvalidOperationException("invalid_id_size");

            if (nodeInfo.Equals(myNodeInfo))
                this.RaiseWarning("my_node_info".GetLogMessage(), 5);
            else
            {
                int distanceLevel = GetDistanceLevel(nodeInfo.Id);
                lock (kbuckets[distanceLevel])
                    if (isValid)
                        if (kbuckets[distanceLevel].Contains(nodeInfo))
                        {
                            kbuckets[distanceLevel].Remove(nodeInfo);
                            kbuckets[distanceLevel].Add(nodeInfo);
                        }
                        else
                        {
                            if (kbuckets[distanceLevel].Count >= K)
                            {
                                ICremliaNodeInfomation pingNodeInfo = kbuckets[distanceLevel][0];
                                this.StartTask("update_node_state", "update_node_state", () =>
                                {
                                    bool isResponded = ReqPing(pingNodeInfo);

                                    lock (kbuckets[distanceLevel])
                                        if (kbuckets[distanceLevel][0] == pingNodeInfo)
                                            if (isResponded)
                                            {
                                                kbuckets[distanceLevel].RemoveAt(0);
                                                kbuckets[distanceLevel].Add(pingNodeInfo);
                                            }
                                            else
                                            {
                                                kbuckets[distanceLevel].RemoveAt(0);
                                                kbuckets[distanceLevel].Add(nodeInfo);
                                            }
                                });
                            }
                            else
                                kbuckets[distanceLevel].Add(nodeInfo);
                        }
                    else if (kbuckets[distanceLevel].Contains(nodeInfo))
                        kbuckets[distanceLevel].Remove(nodeInfo);

            }
        }

        public void UpdateNodeStateWhenJoin(ICremliaNodeInfomation nodeInfo)
        {
            if (nodeInfo.Id.Size != keySpace)
                throw new InvalidOperationException("invalid_id_size");

            if (nodeInfo.Equals(myNodeInfo))
                this.RaiseWarning("my_node_info".GetLogMessage(), 5);
            else
            {
                int distanceLevel = GetDistanceLevel(nodeInfo.Id);
                if (kbuckets[distanceLevel].Contains(nodeInfo))
                {
                    kbuckets[distanceLevel].Remove(nodeInfo);
                    kbuckets[distanceLevel].Add(nodeInfo);
                }
                else if (kbuckets[distanceLevel].Count >= K)
                {
                    kbuckets[distanceLevel].RemoveAt(0);
                    kbuckets[distanceLevel].Add(nodeInfo);
                }
                else
                    kbuckets[distanceLevel].Add(nodeInfo);
            }
        }
    }

    public abstract class CremliaMessageBase { }

    public class PingReqMessage : CremliaMessageBase { }

    public class PingResMessage : CremliaMessageBase { }

    public class StoreReqMessage : CremliaMessageBase
    {
        public StoreReqMessage(ICremliaId _id, byte[] _data)
        {
            id = _id;
            data = _data;
        }

        public readonly ICremliaId id;
        public readonly byte[] data;
    }

    public class FindNodesReqMessage : CremliaMessageBase
    {
        public FindNodesReqMessage(ICremliaId _id)
        {
            id = _id;
        }

        public readonly ICremliaId id;
    }

    public class NeighborNodesMessage : CremliaMessageBase
    {
        public NeighborNodesMessage(ICremliaNodeInfomation[] _nodeInfos)
        {
            nodeInfos = _nodeInfos;
        }

        public readonly ICremliaNodeInfomation[] nodeInfos;
    }

    public class FindValueReqMessage : CremliaMessageBase
    {
        public FindValueReqMessage(ICremliaId _id)
        {
            id = _id;
        }

        public readonly ICremliaId id;
    }

    public class ValueMessage : CremliaMessageBase
    {
        public ValueMessage(byte[] _data)
        {
            data = _data;
        }

        public readonly byte[] data;
    }

    public class GetIdsAndValuesReqMessage : CremliaMessageBase { }

    public class IdsAndValuesMessage : CremliaMessageBase
    {
        public IdsAndValuesMessage(Tuple<ICremliaId, byte[]>[] _idsAndValues)
        {
            idsAndValues = _idsAndValues;
        }

        public readonly Tuple<ICremliaId, byte[]>[] idsAndValues;
    }

    public class MultipleReturn<T, U>
    {
        public MultipleReturn(T _value1)
        {
            Value1 = _value1;
        }

        public MultipleReturn(U _value2)
        {
            Value2 = _value2;
        }

        public T Value1 { get; private set; }
        public U Value2 { get; private set; }

        public bool IsValue1
        {
            get { return Value1 != null; }
        }

        public bool IsValue2
        {
            get { return Value2 != null; }
        }
    }

    #region misc

    public static class Extension
    {
        public class TaskData : Extension.TaskInformation
        {
            public readonly int Number;
            public readonly DateTime StartedTime;

            public TaskData(Extension.TaskInformation _taskInfo, int _number)
                : base(_taskInfo.Name, _taskInfo.Descption, _taskInfo.Action)
            {
                Number = _number;
                StartedTime = DateTime.Now;
            }
        }

        public class Tasker
        {
            private readonly object tasksLock = new object();
            private readonly List<TaskStatus> tasks;
            public TaskData[] Tasks
            {
                get
                {
                    lock (tasksLock)
                        return tasks.Select((e) => e.Data).ToArray();
                }
            }

            public class TaskStatus
            {
                public readonly TaskData Data;
                public readonly Thread Thread;

                public TaskStatus(TaskData _data, Thread _thread)
                {
                    Data = _data;
                    Thread = _thread;
                }
            }

            public Tasker()
            {
                tasks = new List<TaskStatus>();
            }

            public event EventHandler TaskStarted = delegate { };
            public event EventHandler TaskEnded = delegate { };

            public void New(TaskData task)
            {
                Thread thread = new Thread(() =>
                {
                    try
                    {
                        task.Action();
                    }
                    catch (Exception ex)
                    {
                        this.RaiseError("task".GetLogMessage(), 5, ex);
                    }
                    finally
                    {
                        TaskEnded(this, EventArgs.Empty);
                    }
                });
                thread.IsBackground = true;
                thread.Name = task.Name;

                lock (tasksLock)
                    tasks.Add(new TaskStatus(task, thread));

                TaskStarted(this, EventArgs.Empty);

                thread.Start();
            }

            public void Abort(TaskData abortTask)
            {
                TaskStatus status = null;
                lock (tasksLock)
                {
                    if ((status = tasks.Where((e) => e.Data == abortTask).FirstOrDefault()) == null)
                        throw new InvalidOperationException("task_not_found");
                    tasks.Remove(status);
                }
                status.Thread.Abort();

                this.RaiseNotification("task_aborted".GetLogMessage(), 5);
            }

            public void AbortAll()
            {
                lock (tasksLock)
                {
                    foreach (var task in tasks)
                        task.Thread.Abort();
                    tasks.Clear();
                }

                this.RaiseNotification("all_tasks_aborted".GetLogMessage(), 5);
            }
        }

        static Extension()
        {
            Tasker tasker = new Tasker();
            int taskNumber = 0;

            Extension.Tasked += (sender, e) => tasker.New(new TaskData(e, taskNumber++));
        }

        public class TaskInformation
        {
            public readonly string Name;
            public readonly string Descption;
            public readonly Action Action;

            public TaskInformation(string _name, string _description, Action _action)
            {
                Action = _action;
                Name = _name;
                Descption = _description;
            }
        }

        public static event EventHandler<TaskInformation> Tasked = delegate { };

        public static void StartTask<T>(this T self, string name, string description, Action action)
        {
            Tasked(self.GetType(), new TaskInformation(name, description, action));
        }

        public class LogInfomation
        {
            public readonly Type Type;
            public readonly string Message;
            public readonly int Level;
            public readonly string[] Arguments;

            public LogInfomation(Type _type, string _message, int _level, string[] _arguments)
            {
                Type = _type;
                Message = _message;
                Level = _level;
                Arguments = _arguments;
            }

            public LogInfomation(Type _type, string _message, int _level) : this(_type, _message, _level, null) { }
        }

        public static event EventHandler<LogInfomation> Notified = delegate { };
        public static event EventHandler<LogInfomation> Warned = delegate { };
        public static event EventHandler<LogInfomation> Errored = delegate { };

        public static void RaiseNotification<T>(this T self, string message, int level)
        {
            Notified(self.GetType(), new LogInfomation(self.GetType(), message, level));
        }

        public static void RaiseWarning<T>(this T self, string message, int level)
        {
            Warned(self.GetType(), new LogInfomation(self.GetType(), message, level));
        }

        public static void RaiseError<T>(this T self, string message, int level)
        {
            Errored(self.GetType(), new LogInfomation(self.GetType(), message, level));
        }

        public static void RaiseError<T>(this T self, string message, int level, Exception ex)
        {
            Errored(self.GetType(), new LogInfomation(self.GetType(), string.Join(Environment.NewLine, message, ex.CreateMessage(0)), level));
        }

        public static string GetLogMessage(this string rawMessage)
        {
            return rawMessage;
        }

        public static string GetLogMessage(this string rawMessage, params string[] arguments)
        {
            return rawMessage;
        }

        private static Random random = new Random();

        public static int RandomNum(this int i)
        {
            return random.Next(i);
        }

        public static string CreateMessage(this Exception ex, int level)
        {
            string space = " ".Repeat(level * 4);
            string exception = space + "Exception: " + ex.GetType().ToString();
            string message = space + "Message: " + ex.Message;
            string stacktrace = space + "StackTrace: " + Environment.NewLine + ex.StackTrace;

            string thisException;
            if (ex is SocketException)
            {
                SocketException sex = ex as SocketException;

                string errorCode = "ErrorCode: " + sex.ErrorCode.ToString();
                string socketErrorCode = "SocketErrorCode: " + sex.SocketErrorCode.ToString();

                thisException = string.Join(Environment.NewLine, exception, message, errorCode, socketErrorCode, stacktrace);
            }
            else
                thisException = string.Join(Environment.NewLine, exception, message, stacktrace);

            if (ex.InnerException == null)
                return thisException;
            else
            {
                string splitter = "-".Repeat(80);
                string innerexception = ex.InnerException.CreateMessage(level + 1);

                return string.Join(Environment.NewLine, thisException, splitter, innerexception);
            }
        }

        public static string Repeat(this string str, int count)
        {
            return string.Concat(Enumerable.Repeat(str, count).ToArray());
        }
    }

    #endregion misc
}