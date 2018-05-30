#ifndef _READINGSET_H
#define _READINGSET_H
/*
 * FogLAMP storage client.
 *
 * Copyright (c) 2018 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 */
#include <string>
#include <string.h>
#include <sstream>
#include <iostream>
#include <reading.h>
#include <rapidjson/document.h>
#include <vector>

/**
 * Reading set class
 *
 * A specialised container for a set of readings that allows
 * creation from a JSON document.
 */
class ReadingSet {
	public:
		ReadingSet(const std::string& json);
		~ReadingSet();

		unsigned int			getCount() const { return m_count; };
		const Reading			*operator[] (const unsigned int idx) {
							return m_readings[idx];
						};
		// Return a reference of m_readings
		std::vector<Reading *>&		getAllReadings() {
							return m_readings;
						};
	private:
		unsigned int			m_count;
		std::vector<Reading *>		m_readings;

};

/**
 * JSONReading class
 *
 * A specialised reading class that allows creation from a JSON document
 */
class JSONReading : public Reading {
	public:
		JSONReading(const rapidjson::Value& json);
};

class ReadingSetException : public std::exception
{
	public:
		ReadingSetException(const char *what)
		{
			m_what = strdup(what);
		};
		~ReadingSetException()
		{
			if (m_what)
				free(m_what);
		};
		virtual const char *what() const throw()
		{
			return m_what;
		};
	private:
		char *m_what;
};
#endif

