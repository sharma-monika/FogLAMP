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
#include <thread>
#include <north_plugin.h>
#include <reading.h>


// SendingProcess class
class SendingProcess : public FogLampProcess
{
	public:
		// Constructor:
		SendingProcess(int argc, char** argv);

		// Destructor
		~SendingProcess();

		int getStreamId() const;
		bool isRunning() const;
		void stopRunning();
		bool loadPlugin(std::string& pluginName);

	public:
		std::vector<std::vector <Reading*> >	m_buffer;
		std::thread				*m_thread_load;
		std::thread				*m_thread_send;
		NorthPlugin				*m_plugin;

	private:
		bool			m_running;
		int 			m_stream_id;
};

#endif
