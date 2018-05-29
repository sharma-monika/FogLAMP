/*
 * FogLAMP OMF north plugin.
 *
 * Copyright (c) 2018 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Massimiliano Pinto
 */
#include <plugin_api.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string>
#include <logger.h>
#include <plugin_exception.h>
#include <iostream>
#include <omf.h>
#include <simple_https.h>

using namespace std;

/**
 * The OMF plugin interface
 */
extern "C" {

/**
 * The C API plugin information structure
 */
static PLUGIN_INFORMATION info = {
	"OMF",			// Name
	"1.0.0",		// Version
	0,			// Flags
	PLUGIN_TYPE_NORTH,	// Type
	"1.0.0"			// Interface version
};

/**
 * Plugin specific default configuration: TODO
 */
static string plugin_default_config = "\"URL\": "
					"{ \"description\": \"The URL of the PI Connector to send data to\", "
					"\"default\": \"https://pi-server:5460/ingress/messages\", "
    					"\"value\": \"https://192.168.1.145:5460/ingress/messages\", "
   					"\"type\": \"string\" }";

/**
 * Historian PI Server connector info
 */
typedef struct
{
	SimpleHttps	*sender;  // HTTPS connection
	OMF 		*omf;     // OMF data protocol
} CONNECTOR_INFO;

static CONNECTOR_INFO connector_info;

/**
 * Return the information about this plugin: TODO
 */
PLUGIN_INFORMATION *plugin_info()
{
	return &info;
}

/**
 * Return specific plugin configuration: TODO
 */
const string& plugin_config()
{
	return plugin_default_config;
}

/**
 * Initialise the plugin with configuration.
 *
 * This funcion is called to get the plugin handle.
 */
PLUGIN_HANDLE plugin_init(const std::string& configData)
{
	// TODO: handle the configData passed

	// Allocate the HTTPS handler for "Hostname : port"
	connector_info.sender = new SimpleHttps("127.0.0.1:443");

	string path("/ingress/messages");
	string typeId("1534");
	string producerToken("omf_translator_1534");

	// Allocate the OMF data protocol
	connector_info.omf = new OMF(*connector_info.sender,
				     path,
				     typeId,
				     producerToken);

	// TODO: return a more useful data structure for pluin handle
	string* handle = new string("Init done");

	return (PLUGIN_HANDLE)handle;
}

/**
 * Send Readings data to historian server
 */
uint32_t plugin_send(const PLUGIN_HANDLE handle,
		     const vector<Reading *> readings)
{
	return connector_info.omf->sendToServer(readings);
}

/**
 * Shutdown the plugin
 *
 * Delete allocated data
 *
 * @param handle    The plugin handle
 */
void plugin_shutdown(PLUGIN_HANDLE handle)
{
	// Delete connector data
	delete connector_info.sender;
	delete connector_info.omf;

	// Delete the handle
	string* data = (string *)handle;
        delete data;
}

// End of extern "C"
};
