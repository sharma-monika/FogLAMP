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

int main(int argc, char** argv)
{
	SendingProcess* handle;
	// Instantiate the SendingProcess class
	try
	{
		SendingProcess sendigProcess(argc, argv);
		handle = &sendigProcess;
	}
	catch (const std::exception & e)
	{
		cerr << "Exception in " << argv[0] << " : " << e.what() << endl;
		exit(1);
	}

	// Get current logger
	Logger* logger = Logger::getLogger();

	// Start processing
	logger->info("SendingProcess class initialised with StreamID %d", handle->getStreamId());

	/**
	 * Add some processing here
	 */

	// End processing
	logger->info("SendingProcess is succesfully exiting");

	exit(0);
}
