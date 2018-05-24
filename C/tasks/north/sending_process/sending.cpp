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

//Define the type of the plugin managed by the Sending Process
#define PLUGIN_TYPE "north"

// Types of sources for the data blocks
#define DATA_SOURCE_READINGS   "readings"
#define DATA_SOURCE_STATISTICS "statistics"
#define DATA_SOURCE_AUDIT      "audit"

// Audit code to use
#define AUDIT_CODE "STRMN"

// Configuration retrieved from the Configuration Manager
#define CONFIG_CATEGORY_NAME "SEND_PR"
#define CONFIG_CATEGORY_DESCRIPTION "Configuration of the Sending Process"
// Complete the JSON data
#define CONFIG_DEFAULT "{}"

// Buffer max elements
#define DATA_BUFFER_ELMS 10

using namespace std;

// Mutex for m_buffer access
std::mutex	cReadMutex;

// Destructor
SendingProcess::~SendingProcess()
{
	delete m_thread_load;
	delete m_thread_send;
}

// Constructor
SendingProcess::SendingProcess(int argc, char** argv) : FogLampProcess(argc, argv)
{
	// Get streamID from command line
	m_stream_id = atoi(this->getArgValue("--stream-id=").c_str());

	// Set buffer
	m_buffer.resize(DATA_BUFFER_ELMS);

	// Mark running state
	m_running = true;

	Logger::getLogger()->info("SendingProcess class init with stream id (%d), buffer elms (%d)",
				  m_stream_id,
				  DATA_BUFFER_ELMS);
}

// Return the stream id
int SendingProcess::getStreamId() const
{
	return m_stream_id;
}

// Return running state
bool SendingProcess::isRunning() const
{
	return m_running;
}

// Set running stop state
void SendingProcess::stopRunning()
{
	m_running = false;
}
