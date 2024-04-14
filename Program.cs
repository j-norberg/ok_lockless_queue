using System.Collections.Concurrent;
using System.Diagnostics;
using okLockless;

// try the lockless containers

class Program
{
    static ConcurrentQueue<string> sQueue = new ConcurrentQueue<string>();
//    static LocklessQueue<string> sQueue = new LocklessQueue<string>();

    static List<string> CreateUniqueueStrings(int ind, int count)
    {
        var sl = new List<string>();
        for (int i = 0; i < count; ++i)
        {
            sl.Add($"{ind}-{i}");
        }
        return sl;
    }

    static volatile int _totalProduced = 0;
    static volatile int _totalConsumed = 0;

    static void TestProducer(Random r, List<string> sl)
    {
        int mask = (1024 * 1024) - 1;
        while (sl.Count > 0)
        {
            // get random
            int i = r.Next(sl.Count);
            var s = sl[i];

            // remove
            int li = sl.Count - 1;
            sl[i] = sl[li];
            sl.RemoveAt(li);

            sQueue.Enqueue(s);

            int counter = Interlocked.Increment(ref _totalProduced);
            if ((counter & mask) == 0)
            {
                Console.WriteLine($"Producing {counter/mask}M");
            }
        }

        Console.WriteLine("Done Producing");
    }

    static void TestConsumer(int limit, List<string> sl)
    {
        int mask = (1024 * 1024) - 1;

        int emptyCounter = 0;
        for (; ; )
        {
            if (sQueue.TryDequeue(out string s))
            {
                // no sleep in this path since there might be more to dequeue
                sl.Add(s);
                int counter = Interlocked.Increment(ref _totalConsumed);
                if ((counter & mask) == 0)
                {
                    Console.WriteLine($"Consuming {counter/mask}M");
                }

                emptyCounter = 0; // reset this
            }
            else
            {
                SpinWait sw = new SpinWait();
                sw.SpinOnce();

//                Console.WriteLine($"Consumer failed to pop because empty, handled");
 
                ++emptyCounter;
                if (emptyCounter > 1000)
                {
                    Console.WriteLine("Fail Consuming because 1000 failed dequeue in a row");
                    break; // failure because very empty
                }
            }

            if (_totalConsumed >= limit)
            {
                break;
            }
        }

        Console.WriteLine("Done Consuming");
    }

    static void TestCorrectness()
    {
        long mem1 = GC.GetTotalMemory(true);

        Random r = new Random();

        List<List<string>> producerSL = new List<List<string>>();
        List<List<string>> consumerSL = new List<List<string>>();

        int limit = 0;

//        const int count = 2;
        const int count = 16;

        for (int i = 0; i < count; ++i)
        {
            int s = 1 * 1000 * 1000; // 1M each
            limit += s;
            producerSL.Add(CreateUniqueueStrings(i, s));
            consumerSL.Add(new List<string>());
        }
        
        Stopwatch sw = new Stopwatch();

#if true

        // produce and consume interleaved
        Action[] actions = new Action[count*2];
        for (int i = 0; i < count; ++i)
        {
            var pL = producerSL[i];
            var cL = consumerSL[i];
            Action tP = () => TestProducer(r, pL);
            Action tC = () => TestConsumer(limit, cL);
            actions[i] = tP;
            actions[i+count] = tC;
        }

        sw.Start();
        Parallel.Invoke(actions);
        sw.Stop();
        actions = null;

#else

        // produce and consume separately
        Action[] actions1 = new Action[count];
        Action[] actions2 = new Action[count];
        for (int i = 0; i < count; ++i)
        {
            var pL = producerSL[i];
            Action tP = () => TestProducer(r, pL);
            actions1[i] = tP;

            var cL = consumerSL[i];
            Action tC = () => TestConsumer(limit, cL);
            actions2[i] = tC;
        }

        sw.Start();
        Parallel.Invoke(actions1);
        Parallel.Invoke(actions2);
        sw.Stop();

        actions1 = null;
        actions1 = null;

#endif

        Console.WriteLine("Elapsed={0}", sw.Elapsed);

        // all producers are done
        for (int i = 0; i < count; ++i)
        {
            if (producerSL[i].Count != 0)
                throw new Exception("ERROR producer did not use all strings");
            producerSL[i] = null;
        }

        // ensure uniqueue
        var sl = new List<string>();
        for (int i = 0; i < count; ++i)
        {
            Console.WriteLine($"consumer {i} took {consumerSL[i].Count} strings");

            sl.AddRange(consumerSL[i]);
            consumerSL[i].Clear();
            consumerSL[i] = null;
        }

        if (sl.Count == limit)
            Console.WriteLine("GOOD consumers did take all strings");
        else
            throw new Exception("ERROR consumers did not take all strings");

        Console.WriteLine("Testing collisions...");

        HashSet<string> set = new HashSet<string>();
        foreach (string s in sl)
        {
            if (set.Contains(s))
                Console.WriteLine("Not unique={0}", s);
            set.Add(s);
        }

        if (set.Count == sl.Count)
            Console.WriteLine("GOOD No collision");

        // clear
        producerSL.Clear();
        producerSL = null;
        consumerSL.Clear();
        consumerSL = null;
        sl.Clear();
        sl = null;

//        sQueue.ResetFreeList();

        long mem2 = GC.GetTotalMemory(true);
        long bytesDiff = mem2 - mem1;
        float megDiff = (float)bytesDiff / (float)(1024 * 1024);
        Console.WriteLine($"Mem diff = {megDiff} Mb");


        Console.WriteLine("Done");
    }

    static void Main(string[] args)
    {
        try
        {
            TestCorrectness();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
    }
}
