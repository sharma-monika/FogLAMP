/*
 * FogLAMP process class
 *
 * Copyright (c) 2018 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Massimiliano Pinto
 */

#include <sending.h>
#include <condition_variable>
/**
 * The sending process is run according to a schedule in order to send reading data
 * to the historian, e.g. the PI system.
 * It’s role is to implement the rules as to what needs to be sent and when,
 * extract the data from the storage subsystem and stream it to the north
 * for sending to the external system.
 * The sending process does not implement the protocol used to send the data,
 * that is devolved to the translation plugin in order to allow for flexibility
 * in the translation process.
 */

using namespace std;

// Buffer max elements
#define DATA_BUFFER_ELMS 10

// Mutex for m_buffer access
mutex      readMutex;
// Mutex for thread idle time
mutex	waitMutex;

condition_variable cond_var;

// Load data from storage
static void loadDataThread(SendingProcess *loadData);
// Send data from historian
static void sendDataThread(SendingProcess *sendData);



int main(int argc, char** argv)
{
	try
	{
		// Instantiate SendingProcess class
		SendingProcess sendigProcess(argc, argv);

		// Launch the load thread
		sendigProcess.m_thread_load = new thread(loadDataThread, &sendigProcess);
		// Launch the send thread
		sendigProcess.m_thread_send = new thread(sendDataThread, &sendigProcess);

		// Check running time && handle signals
		// Simulation with sleep
		sleep(120);

		// End processing
		sendigProcess.stopRunning();

		// threads execution has completed.
		sendigProcess.m_thread_load->join();
		sendigProcess.m_thread_send->join();
	}
	catch (const std::exception & e)
	{
		cerr << "Exception in " << argv[0] << " : " << e.what() << endl;

		// Return failure for class instance
		exit(1);
	}

	// Return success
	exit(0);
}

/**
 * Thread to load data from the storage layer.
 *
 * @param loadData    pointer to SendingProcess instance
 */
static void loadDataThread(SendingProcess *loadData)
{
        unsigned int    readIdx = 0;

        while (loadData->isRunning())
        {
                int canLoad;

                if (readIdx >= DATA_BUFFER_ELMS)
                {
                        readIdx = 0;
                        continue;
                }

		// Check whether m_buffer[readIdx] is empty
		// Access is protected by a mutex.
                readMutex.lock();
                canLoad = loadData->m_buffer[readIdx].empty();
                readMutex.unlock();

                if (!canLoad)
                {
                        Logger::getLogger()->info("-- loadDataThread: " \
                                                  "(stream id %d), readIdx %u, buffer is NOT empty, waiting ...",
                                                  loadData->getStreamId(),
                                                  readIdx);

			// Load thread is put on hold
			unique_lock<mutex> lock(waitMutex);
			cond_var.wait(lock);

			Logger::getLogger()->info("-- loadDataThread is now ready");
                }
                else
                {
                        Logger::getLogger()->info("-- loadDataThread: (stream id %d), readIdx %u. Loading data ...",
                                                  loadData->getStreamId(),
                                                  readIdx);

                        // Load data & transform data: simulating via sleep
                        sleep(4);

			// Add data to m_buffer[readIdx]
			// Access is protected by a mutex
                        readMutex.lock();

                        loadData->m_buffer[readIdx] = "Data Slot ";
                        loadData->m_buffer[readIdx] += to_string(readIdx);

                        readMutex.unlock();

                        Logger::getLogger()->info("-- loadDataThread: (stream id %d), readIdx %u. Loading done, data Buffer SET",
                                                  loadData->getStreamId(),
                                                  readIdx);
                        readIdx++;

			// Unlock the sendData thread
			unique_lock<mutex> lock(waitMutex);
			cond_var.notify_one();
                }
        }
}

/**
 * Thread to send data to historian service
 *
 * @param loadData    pointer to SendingProcess instance
 */
static void sendDataThread(SendingProcess *sendData)
{
        unsigned int    sendIdx = 0;

        while (sendData->isRunning())
        {
                int canSend;

                if (sendIdx >= DATA_BUFFER_ELMS)
                {
                        sendIdx = 0;
                        continue;
                }

		// Check whether m_buffer[sendIdx] is empty
		// Access is protected by a mutex.
                readMutex.lock();
                canSend = sendData->m_buffer[sendIdx].empty();
                readMutex.unlock();

                if (canSend)
                {
                        Logger::getLogger()->info("++ sendDataThread: " \
                                                  "(stream id %d), sendIdx %u, buffer is empty, waiting ...",
                                                  sendData->getStreamId(),
                                                  sendIdx);

			// Send thread is put on hold
                        unique_lock<mutex> lock(waitMutex);
                        cond_var.wait(lock);

			Logger::getLogger()->info("++ sendDataThread is now ready");
                }
                else
                {
                        Logger::getLogger()->info("++ sendDataThread: " \
                                                  "(stream id %d), sendIdx %u. Sending data ...",
                                                  sendData->getStreamId(),
                                                  sendIdx);

                        // Send Data: simulating via sleep
                        sleep(15);

			// Emptying data in m_buffer[sendIdx]
			// Access is protected by a mutex
                        readMutex.lock();

                        sendData->m_buffer[sendIdx] = "";

                        readMutex.unlock();

                        Logger::getLogger()->info("++ sendDataThread: " \
                                                  "(stream id %d), sendIdx %u. Sending done. Data Buffer SET to empty",
                                                  sendData->getStreamId(),
                                                  sendIdx);

                        sendIdx++;

			// Unlock the loadData thread
			unique_lock<mutex> lock(waitMutex);
			cond_var.notify_one();
                }
        }
}
