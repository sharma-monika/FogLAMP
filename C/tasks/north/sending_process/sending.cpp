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

using namespace std;

// Destructor
SendingProcess::~SendingProcess()
{
}

// Constructor
SendingProcess::SendingProcess(int argc, char** argv) : FogLampProcess(argc, argv)
{
	// Get streamID from command line
	m_stream_id = atoi(this->getArgValue("--stream-id=").c_str());
}

int SendingProcess::getStreamId() const
{
	return m_stream_id;
}
