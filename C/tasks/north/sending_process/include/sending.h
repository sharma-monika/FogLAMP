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

		int			getStreamId() const;
		bool			isRunning() const;
		void			stopRunning();
		bool			loadPlugin(const std::string& pluginName);
		void			stop();
		void			setLastSentId(unsigned long id) { m_last_sent_id = id; };
		unsigned long		getLastSentId() const { return m_last_sent_id; };
		unsigned long		getSentReadings() const { return m_tot_sent; };
		bool			updateSentReadings(unsigned long num) {
					m_tot_sent += num;
					return m_tot_sent;
		};
		void			resetSentReadings() { m_tot_sent = 0; };
		void			updateDatabaseCounters();
		bool			getLastSentReadingId();
		const std::string	fetchConfiguration();
		void			setDuration(unsigned int val) { m_duration = val; };
		unsigned int		getDuration() const { return m_duration; };
		void			setSleepTime(unsigned long val) { m_sleep = val; };
		unsigned int		getSleepTime() const { return m_sleep; };
		bool			getUpdateDb() const { return m_update_db; };
		bool			setUpdateDb(bool val) {
						    m_update_db = val;
						    return m_update_db;
		};
		unsigned long		getReadBlockSize() const { return m_block_size; };
		void			setReadBlockSize(unsigned long size) { m_block_size = size; };

	public:
		std::vector<ReadingSet *>	m_buffer;
		std::thread			*m_thread_load;
		std::thread			*m_thread_send;
		NorthPlugin			*m_plugin;

	private:
		bool				m_running;
		int 				m_stream_id;
		unsigned long			m_last_sent_id;
		unsigned long			m_tot_sent;
		unsigned int			m_duration;
		unsigned long			m_sleep;
		unsigned long			m_block_size;
		bool				m_update_db;
};

#endif
