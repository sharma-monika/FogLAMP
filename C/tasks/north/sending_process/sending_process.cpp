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
#include <reading_set.h>
#include <plugin_manager.h>
#include <plugin_api.h>
#include <plugin.h>

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

// Thread sleep (milliseconds) when no data from storage layer
#define TASK_FETCH_SLEEP 1000

// historian plugin to load
#define PLUGIN_NAME "omf"

// Sendinf process configuration
static const string sendingProcessConfiguration = "\"plugin\" : { \"type\" : \"string\", \"value\" : \"omf\", "
							"\"default\" : \"omf\", \"description\" : \"Python module name of the plugin to load\" }" ;

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
	string pluginName = PLUGIN_NAME;

	try
	{
		// Instantiate SendingProcess class
		SendingProcess sendingProcess(argc, argv);

		if (!sendingProcess.loadPlugin(pluginName))
		{
			Logger::getLogger()->fatal("Failed to load north plugin '%s'.", pluginName.c_str());
			exit(2);
		}

		// Build JSON merged configuration (sendingProcess + pluginConfig
		string mergedConfiguration("{ ");
		// Get plugin default config
		mergedConfiguration.append(sendingProcess.m_plugin->config());
		mergedConfiguration += ", ";
		mergedConfiguration.append(sendingProcessConfiguration);
		mergedConfiguration += " }";

		// Init plugin with merged configuration
		sendingProcess.m_plugin->init(mergedConfiguration);

		// Launch the load thread
		sendingProcess.m_thread_load = new thread(loadDataThread, &sendingProcess);
		// Launch the send thread
		sendingProcess.m_thread_send = new thread(sendDataThread, &sendingProcess);

		// Check running time && handle signals
		// Simulation with sleep
		sleep(40);

		// End processing
		sendingProcess.stop();
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
                if (readIdx >= DATA_BUFFER_ELMS)
                {
                        readIdx = 0;
                }

		/**
		 * Check whether m_buffer[readIdx] is NULL or contains a ReadingSet
		 *
		 * Access is protected by a mutex.
		 */
                readMutex.lock();
                ReadingSet *canLoad = loadData->m_buffer.at(readIdx);
                readMutex.unlock();

                if (canLoad)
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
                        // Load data from storage client (id >= and 10 rows)
			ReadingSet* readings = NULL;
			try
			{
				readings = loadData->getStorageClient()->readingFetch(0, 10);
				// Delay for test
				usleep(7000);
			}
			catch (ReadingSetException* e)
			{
				Logger::getLogger()->error("SendingProcess loadData(): ReadingSet Exception '%s'", e->what());
			}
			catch (std::exception& e)
			{
				Logger::getLogger()->error("SendingProcess loadData(): Generic Exception: '%s'", e.what());
			}

			// Add available data to m_buffer[readIdx]
			if (readings != NULL && readings->getCount())
			{		
				/**
				 * The buffer access is protected by a mutex
				 */
                	        readMutex.lock();

				/**
				 * Set now the buffer at index to ReadingSet pointer
				 * Note: the ReadingSet pointer will be deleted by
				 * - the sending thread when processin it
				 * OR
				 * at program exit by a cleanup routine
				 */
	                      	loadData->m_buffer.at(readIdx) = readings;

                        	Logger::getLogger()->info("-- loadDataThread: (stream id %d), readIdx %u. Loading done, data Buffer SET with %d readings",
							  loadData->getStreamId(),
							  readIdx, readings->getCount());
                        	readMutex.unlock();

                        	readIdx++;

				// Unlock the sendData thread
				unique_lock<mutex> lock(waitMutex);
				cond_var.notify_one();
			}
			else
			{
				// Error or no data read: just wait
				this_thread::sleep_for(chrono::milliseconds(TASK_FETCH_SLEEP));
			}
                }
        }
	/**
	 * The loop is over: unlock the sendData thread
	 */
	unique_lock<mutex> lock(waitMutex);
	cond_var.notify_one();
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
                if (sendIdx >= DATA_BUFFER_ELMS)
                {
                        sendIdx = 0;
                }

		/*
		 * Check whether m_buffer[sendIdx] is NULL or contains ReadinSet data.
		 * Access is protected by a mutex.
		 */
                readMutex.lock();
                ReadingSet *canSend = sendData->m_buffer.at(sendIdx);
                readMutex.unlock();

                if (canSend == NULL)
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
			/**
			 * Send the buffer content ( const vector<Readings *>& )
			 * using m_plugin->send(data)
			 * Readings data by getAllReadings() will be
			 * transformed using historian protocol and then sent to destination.
			 */

			// Get Readings data reference
			const vector<Reading *> &readingData = sendData->m_buffer.at(sendIdx)->getAllReadings();
			// Process & Send the Readings
			uint32_t sentReadings = sendData->m_plugin->send(readingData);

			if (sentReadings)
			{
				/**
				 * Sending done, emptying data in m_buffer[sendIdx].
				 * The buffer access is protected by a mutex.
				 */
				readMutex.lock();

				delete sendData->m_buffer.at(sendIdx);
				sendData->m_buffer.at(sendIdx) = NULL;

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
			else
			{
				Logger::getLogger()->error("++ sendDataThread: Error while sending" \
							   "(stream id %d), sendIdx %u. N. (%d readings)",
							   sendData->getStreamId(),
							   sendIdx,
							   sendData->m_buffer[sendIdx]->getCount());

				// Error: just wait & continue
				this_thread::sleep_for(chrono::milliseconds(TASK_FETCH_SLEEP));
			}

                }
        }

	/**
	 * The loop is over: unlock the loadData thread
	 */
	unique_lock<mutex> lock(waitMutex);
	cond_var.notify_one();
}
