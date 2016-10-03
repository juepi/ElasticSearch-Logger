# ElasticSearch-Logger
PowerShell Script to gather data either from an MQTT Broker (polling Topics) or FHEM (via Telnet interface) and push it into ElasticSearch by using LogStash (JSON).
The script should be started through a scheduled task (i.e. at system startup), it will run in an endless loop and will gather/import data every quarter of an hour.

MQTT based readouts require a "Status" Topic for each sensor. The topic will be set to "DataUpdated" after a sensor has updated it's measurements successfully, and will be set to "DataObtained" after the ElasticSearch-Logger has obtained the data from the broker.
This way it's possible to verify if the sensors are still working. All MQTT topics are retained, as the Sensors are only online for a short amount of time (see project "ESPEnvSens").
FHEM based readouts use the "ActionDetector" Device to check if the read out device is "online". This might only work for my HomeMatic device (105155, heating radiator valve actuator).

Software Requirements:
* Mosquitto MQTT Software (https://mosquitto.org/)
* Mosquitto installation path Environment variable set (MOSQUITTO_DIR)
* an MQTT broker (internet or local, authentication not implemented)
* FHEM with telnet enabled, no authentication

Have fun,
Juergen
