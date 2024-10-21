// jnorberg 2024-05-01 (license at end of file)
// Simplified version of ConcurrentQueue (similar interface)

// Clears up memory earlier, a bit simpler, and quite a bit faster in some situations
// often uses 80% of the time
// extreme cases (non-overlapping producers and consumers) only 50%

// The standard version supports multiple producers and multiple consumers MPMC

public class OkLocklessQueue<T>
{
    // How many elements in a Block
    const int _size = 200;

    private class Block
    {
        public volatile T[] _data = new T[_size];

        // because multiple producers, we need a bool to ensure that reader knows that data has been written
        public volatile bool[] _written = new bool[_size];

        // the read and write can go off the edge of the block
        public volatile int _read = 0;
        public volatile int _write = 0;

        // linked list of blocks
        public volatile Block _next = null;
    }

    // Linked List of Blocks
    private volatile Block _head = null; // modified by TryDequeue
    private volatile Block _tail = null; // modified by Enqueue

    public OkLocklessQueue()
    {
        Block newBlock = new Block();
        _head = newBlock;
        _tail = newBlock;
    }

    // This is so much faster than dequeue
    public void Enqueue(T item)
    {
        for (; ; )
        {
            Block currentTail = _tail;
            int insertIndex = Interlocked.Increment(ref currentTail._write) - 1;

            if (insertIndex < _size)
            {
                // best case (most common)
                currentTail._data[insertIndex] = item; // do I need to "Volatile.Write" here?
                Volatile.Write(ref currentTail._written[insertIndex], true);

                // second best (I got the last element, and will create the next block)
                if (insertIndex == (_size - 1))
                {
                    Block newBlock = new Block();
                    currentTail._next = newBlock;
                    _tail = newBlock;

                }

                return;
            }

            // we get here if we were caught after the (index >= _size)
            // just wait until _tail has changed
            // this can be very rare if _size is large and the creation of a new block is fast
            SpinWait sw = new SpinWait();
            while (_tail == currentTail)
            {
                sw.SpinOnce();
            }
        }
    }


    // Has fast-path for full blocks
    public bool TryDequeue(out T result)
    {
        SpinWait sw = new SpinWait();

        for (; ; )
        {
            Block currentHead = _head;
            int readIndex;
            if (currentHead._next != null)
            {
                // this block is full, can read fast path
                // since we can not "catch up" to write, we can use simple increment
                readIndex = Interlocked.Increment(ref currentHead._read) - 1;
            }
            else
            {
                // this block is not full, need to do slow path
                readIndex = currentHead._read;

                // not read all yet
                if (readIndex < _size)
                {
                    sw.Reset();
                    for (; ; )
                    {
                        int writeIndex = currentHead._write;

                        // empty, nothing to do
                        if (readIndex >= writeIndex)
                        {
                            result = default(T);
                            return false;
                        }

                        // try update readIndex
                        int nextReadIndex = readIndex + 1;
                        int originalReadIndex = Interlocked.CompareExchange(ref currentHead._read, nextReadIndex, readIndex);

                        if (originalReadIndex == readIndex)
                            break; // readIndex is good

                        sw.SpinOnce();
                        readIndex = currentHead._read;
                    }
                }
            }

            // at this point "readIndex" should be "ours" (and no other threads)
            if (readIndex < _size)
            {
                // can read back
                // but first wait until it's really written (multiple producers)
                sw.Reset();
                while (Volatile.Read(ref currentHead._written[readIndex]) == false)
                    sw.SpinOnce();

                // there is an element to extract
                result = currentHead._data[readIndex];

                if (readIndex == (_size - 1))
                {
                    // last index have to move "_head", ensure we can move head to something
                    sw.Reset();
                    while (currentHead._next == null)
                        sw.SpinOnce();

                    _head = currentHead._next;
                }

                return true;
            }

            // can only get here if we need to wait for next block
            // Read past the whole block. Need to wait for a new "head" and start over
            sw.Reset();
            while (_head == currentHead)
                sw.SpinOnce();

        }
    }
}

#if false
=============================================================================
    
MIT License

Copyright (c) 2024 Jonas Norberg

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

=============================================================================
#endif
    
