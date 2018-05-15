#ifndef _SENDING_PROCESS_H
#define _SENDING_PROCESS_H

/*
 * FogLAMP process class
 *
 * Copyright (c) 2018 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Massimiliano Pinto
 */

#include <process.h>

// SendingProcess class
class SendingProcess : public FogLampProcess
{
	public:
		// Constructor:
		SendingProcess(int argc, char** argv);

		// Destructor
		~SendingProcess();
};

#endif
