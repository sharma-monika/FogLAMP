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

#define READINGS_FETCH_MAX 1000
#define MAX_EXECUTION_TIME 60

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

                // Fetch last_object sent from foglamp.streams
		if (!sendingProcess.getLastSentReadingId())
		{
			cerr << ">>>>>> Last object id for stream " << sendingProcess.getStreamId() << " NOT found, abort" << endl;
                        exit(3);
		}

		// Launch the load thread
		sendingProcess.m_thread_load = new thread(loadDataThread, &sendingProcess);
		// Launch the send thread
		sendingProcess.m_thread_send = new thread(sendDataThread, &sendingProcess);

		// Check running time && handle signals
		// Simulation with sleep
		sleep(MAX_EXECUTION_TIME);

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
                        // Load data from storage client (id >= lastId and READINGS_FETCH_MAX rows)
			ReadingSet* readings = NULL;
			try
			{
				// Read from storage all readings with id > last sent id
				unsigned long lastReadId = loadData->getLastSentId() + 1;

				// Load READINGS_FETCH_MAX readings
				using namespace std::chrono;

				high_resolution_clock::time_point t1 = high_resolution_clock::now();

				readings = loadData->getStorageClient()->readingFetch(lastReadId, READINGS_FETCH_MAX);

				high_resolution_clock::time_point t2 = high_resolution_clock::now();

				auto duration = duration_cast<microseconds>( t2 - t1 ).count();
				cerr << "FetchReadings[" << readIdx << "] time in microseconds: " << duration << endl;
			}
			catch (ReadingSetException* e)
			{
				Logger::getLogger()->error("SendingProcess loadData(): ReadingSet Exception '%s'", e->what());
			}
			catch (std::exception& e)
			{
				Logger::getLogger()->error("SendingProcess loadData(): Generic Exception: '%s'", e.what());
			}

			// Data fetched from storage layer
			if (readings != NULL && readings->getCount())
			{
				// Update last fetched reading Id
				loadData->setLastSentId(readings->getLastId());

				Logger::getLogger()->info("-- loadDataThread: Read data, Id of last reading element is = [%d]/[%d]",
							  readings->getLastId(), loadData->getLastSentId());

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

                        	readMutex.unlock();

                        	Logger::getLogger()->info("-- loadDataThread: (stream id %d), "
							  "readIdx %u. Loading done, data Buffer SET "
							  "with %d readings",
							  loadData->getStreamId(),
							  readIdx, readings->getCount());

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

	cerr << "Last ID sent: " << loadData->getLastSentId() << endl;

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
	unsigned long totSent = 0;
	unsigned int  sendIdx = 0;

        while (sendData->isRunning())
        {
                if (sendIdx >= DATA_BUFFER_ELMS)
		{

			// Update counters to Database
			sendData->updateDatabaseCounters();

			// numReadings sent so far
			totSent += sendData->getSentReadings();

			// Reset send index
			sendIdx = 0;

			// Reset current sent readings
			sendData->resetSentReadings();	
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
			 * to historian server via m_plugin->send(data).
			 * Readings data by getAllReadings() will be
			 * transformed using historian protocol and then sent to destination.
			 */

			const vector<Reading *> &readingData = sendData->m_buffer.at(sendIdx)->getAllReadings();
			uint32_t sentReadings = sendData->m_plugin->send(readingData);

			if (sentReadings)
			{
				/** Sending done */

				/**
				 * 1- emptying data in m_buffer[sendIdx].
				 * The buffer access is protected by a mutex.
				 */
				readMutex.lock();

				// if persistent commment delete
				delete sendData->m_buffer.at(sendIdx);
				sendData->m_buffer.at(sendIdx) = NULL;

				/** 2- Update sent counter (memory only) */
				sendData->updateSentReadings(sentReadings);

				readMutex.unlock();

				Logger::getLogger()->info("++ sendDataThread: "
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

	cerr << "Tot Readings Sent: " << totSent << endl;

	/**
	 * The loop is over: unlock the loadData thread
	 */
	unique_lock<mutex> lock(waitMutex);
	cond_var.notify_one();
}

/**
 * Update datbaase tables statistics and streams
 * setting last_object id in streams
 * and numReadings sent in statistics
 */
void SendingProcess::updateDatabaseCounters()
{
	// Update counters to Database

	string streamId = to_string(this->getStreamId());

	// Prepare WHERE destination_id = val
	const Condition conditionStream(Equals);
	Where wStreamId("destination_id",
			conditionStream,
			streamId);

	// Prepare last_object = value
	InsertValues lastId;
	lastId.push_back(InsertValue("last_object",
			 (long)this->getLastSentId()));

	// Perform UPDATE foglamp.streams SET destination_id = x WHERE destination_id = y
	this->getStorageClient()->updateTable("streams",
					      lastId,
					      wStreamId);

	cerr << "Table foglamp.streams updated: 'last_object' = " << this->getLastSentId() << endl;

	// Prepare "WHERE SENT_x = val
	const Condition conditionStat(Equals);
	Where wLastStat("key",
			conditionStat,
			string("SENT_" + streamId));

	// Prepare value = value + inc
	ExpressionValues updateValue;
	updateValue.push_back(Expression("value",
			      "+",
			      (int)this->getSentReadings()));

	// Perform UPDATE foglamp.statistics SET value = value + x WHERE key = 'y'
	this->getStorageClient()->updateTable("statistics",
					      updateValue,
					      wLastStat);

	cerr << "Table foglamp.statistics updated: value = value + " << this->getSentReadings() << endl;
}

/**
 * Get last_object id sent for current stream_id
 * Access foglam.streams table.
 *
 * @return true if last_object is found, false otherwise
 */
bool SendingProcess::getLastSentReadingId()
{
	// Fetch last_object sent from foglamp.streams

	bool foundId = false;
	const Condition conditionId(Equals);
	string streamId = to_string(this->getStreamId());
	Where* wStreamId = new Where("destination_id",
				     conditionId,
				     streamId);

	// SELECT * FROM foglamp.streams WHERE destination_id = x
	Query qLastId(wStreamId);

	ResultSet* lastObjectId = this->getStorageClient()->queryTable("streams", qLastId);

	if (lastObjectId != NULL && lastObjectId->rowCount())
	{
		// Get the first row only
		ResultSet::RowIterator it = lastObjectId->firstRow();
		// Access the element
		ResultSet::Row* row = *it;
		if (row)
		{
			// Get column value
			ResultSet::ColumnValue* theVal = row->getColumn("last_object");
			// Set found id
			this->setLastSentId((unsigned long)theVal->getInteger());

			foundId = true;
		}
	}

	return foundId;
}
