#ifndef _SIMPLE_HTTPS_H
#define _SIMPLE_HTTPS_H
/*
 * FogLAMP HTTP Sender wrapper.
 *
 * Copyright (c) 2018 Diamnomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Massimiliano Pinto, Mark Riddoch
 */

#include <string>
#include <vector>
#include <http_sender.h>
#include <client_https.hpp>

using HttpsClient = SimpleWeb::Client<SimpleWeb::HTTPS>;

class SimpleHttps: public HttpSender
{
	public:
		/**
		 * Constructor:
		 * pass host:port, HTTP headers and POST/PUT payload.
		 */
		SimpleHttps(const std::string& host_port);

		// Destructor
		~SimpleHttps();

		/**
		 * HTTP(S) request: pass method and path, HTTP headers and POST/PUT payload.
		 */
		int sendRequest(const std::string& method = std::string(HTTP_SENDER_DEFAULT_METHOD),
				const std::string& path = std::string(HTTP_SENDER_DEFAULT_PATH),
				const std::vector<std::pair<std::string, std::string>>& headers = {},
				const std::string& payload = std::string());
	private:
		std::string	m_host_port;
		HttpsClient	*m_sender;
		
};

#endif
