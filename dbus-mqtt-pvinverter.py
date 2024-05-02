#!/usr/bin/env python

# import normal packages
import platform
import logging
import sys
import os
import sys

if sys.version_info.major == 2:
    import gobject
else:
    from gi.repository import GLib as gobject
import sys
import time
import requests  # for http GET
import configparser  # for config/ini file
from paho.mqtt import client as mqtt_client


# our own packages from victron
sys.path.insert(
    1,
    os.path.join(
        os.path.dirname(__file__),
        "/opt/victronenergy/dbus-systemcalc-py/ext/velib_python",
    ),
)
from vedbus import VeDbusService


class DbusMqttInverterService:
    def __init__(
        self,
        servicename,
        paths,
        productname="Generic mqtt inverer",
        connection="Generic mqtt inverter HTTP JSON service",
    ):
        config = self._getConfig()
        deviceinstance = int(config["DEFAULT"]["Deviceinstance"])
        customname = config["DEFAULT"]["CustomName"]

        self._dbusservice = VeDbusService(
            "{}.http_{:02d}".format(servicename, deviceinstance)
        )
        self._paths = paths

        logging.debug("%s /DeviceInstance = %d" % (servicename, deviceinstance))

        # Create the management objects, as specified in the ccgx dbus-api document
        self._dbusservice.add_path("/Mgmt/ProcessName", __file__)
        self._dbusservice.add_path(
            "/Mgmt/ProcessVersion",
            "Unkown version, and running on Python " + platform.python_version(),
        )
        self._dbusservice.add_path("/Mgmt/Connection", connection)

        # Create the mandatory objects
        self._dbusservice.add_path("/DeviceInstance", deviceinstance)
        # self._dbusservice.add_path('/ProductId', 16) # value used in ac_sensor_bridge.cpp of dbus-cgwacs
        self._dbusservice.add_path(
            "/ProductId", 0xFFFF
        )  # id assigned by Victron Support from SDM630v2.py
        self._dbusservice.add_path("/ProductName", productname)
        self._dbusservice.add_path("/CustomName", customname)
        self._dbusservice.add_path("/Connected", 1)

        self._dbusservice.add_path("/Latency", None)
        self._dbusservice.add_path("/FirmwareVersion", 1)
        self._dbusservice.add_path("/HardwareVersion", 0)
        self._dbusservice.add_path("/Position", int(config["DEFAULT"]["Position"]))
        self._dbusservice.add_path("/Serial", 1234)
        self._dbusservice.add_path("/UpdateIndex", 0)
        self._dbusservice.add_path(
            "/StatusCode", 0
        )  # Dummy path so VRM detects us as a PV-inverter.

        # add path values to dbus
        for path, settings in self._paths.items():
            self._dbusservice.add_path(
                path,
                settings["initial"],
                gettextcallback=settings["textformat"],
                writeable=True,
                onchangecallback=self._handlechangedvalue,
            )

        # last update
        self._lastUpdate = 0

        # add _update function 'timer'
        gobject.timeout_add(250, self._update)  # pause 250ms before the next request

        # add _signOfLife 'timer' to get feedback in log every 5minutes
        gobject.timeout_add(self._getSignOfLifeInterval() * 60 * 1000, self._signOfLife)

        self._power_l1 = 0
        self._power_l2 = 0
        self._power_l3 = 0

        self._connect_mqtt()

    def _getConfig(self):
        config = configparser.ConfigParser()
        config.read("%s/config.ini" % (os.path.dirname(os.path.realpath(__file__))))
        return config

    def _subscribe(self, client: mqtt_client):
        def on_message(client, userdata, msg):
            config = self._getConfig()

            data = msg.payload.decode()

            if msg.topic == str(config["DEFAULT"]["TopicL1"]):
                self._power_l1 = data
            elif msg.topic == str(config["DEFAULT"]["TopicL2"]):
                self._power_l2 = data
            elif msg.topic == str(config["DEFAULT"]["TopicL3"]):
                self._power_l3 = data
            else:
                return

        config = self._getConfig()
        client.subscribe(str(config["DEFAULT"]["TopicBase"]))
        client.on_message = on_message

    def _connect_mqtt(self):
        def on_connect(client, userdata, flags, rc):
            # For paho-mqtt 2.0.0, you need to add the properties parameter.
            # def on_connect(client, userdata, flags, rc, properties):
            if rc == 0:
                print("Connected to MQTT Broker!")
            else:
                print("Failed to connect, return code %d\n", rc)

        def on_disconnect(self, client, userdata, rc):
            FIRST_RECONNECT_DELAY = 1
            RECONNECT_RATE = 2
            MAX_RECONNECT_COUNT = 12
            MAX_RECONNECT_DELAY = 60
            logging.info("Disconnected with result code: %s", rc)
            reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
            while reconnect_count < MAX_RECONNECT_COUNT:
                logging.info("Reconnecting in %d seconds...", reconnect_delay)
                time.sleep(reconnect_delay)

                try:
                    client.reconnect()
                    logging.info("Reconnected successfully!")
                    return
                except Exception as err:
                    logging.error("%s. Reconnect failed. Retrying...", err)

                reconnect_delay *= RECONNECT_RATE
                reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
                reconnect_count += 1
            logging.info(
                "Reconnect failed after %s attempts. Exiting...", reconnect_count
            )

        config = self._getConfig()

        # Set Connecting Client ID
        client = mqtt_client.Client(str(config["DEFAULT"]["MqttClientId"]))

        # For paho-mqtt 2.0.0, you need to set callback_api_version.
        # client = mqtt_client.Client(client_id=client_id, callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2)

        # client.username_pw_set(username, password)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.connect(
            str(config["DEFAULT"]["MqttServer"]), config["DEFAULT"]["MqttPort"]
        )

        return client

    def _update(self):
        try:
            config = self._getConfig()

            for phase, power in [
                ("L1", self._power_l1),
                ("L2", self._power_l2),
                ("L3", self._power_l3),
            ]:
                pre = "/Ac/" + phase

                # total = meter_data["meters"][0]["total"]
                voltage = 230
                current = power / voltage

                self._dbusservice[pre + "/Voltage"] = voltage
                self._dbusservice[pre + "/Current"] = current
                self._dbusservice[pre + "/Power"] = power

                # if power > 0:
                #    self._dbusservice[pre + "/Energy/Forward"] = total / 1000 / 60

            # self._dbusservice["/Ac/Power"] = (
            #     self._power_l1 + self._power_l2 + self._power_l3
            # )
            # self._dbusservice["/Ac/Energy/Forward"] = self._dbusservice[
            #     "/Ac/" + pvinverter_phase + "/Energy/Forward"
            # ]

            # logging
            # logging.debug(
            #     "House Consumption (/Ac/Power): %s" % (self._dbusservice["/Ac/Power"])
            # )
            # logging.debug(
            #     "House Forward (/Ac/Energy/Forward): %s"
            #     % (self._dbusservice["/Ac/Energy/Forward"])
            # )
            logging.debug("---")

            # increment UpdateIndex - to show that new data is available
            index = self._dbusservice["/UpdateIndex"] + 1  # increment index
            if index > 255:  # maximum value of the index
                index = 0  # overflow from 255 to 0
            self._dbusservice["/UpdateIndex"] = index

            # update lastupdate vars
            self._lastUpdate = time.time()
        except Exception as e:
            logging.critical("Error at %s", "_update", exc_info=e)

        # return true, otherwise add_timeout will be removed from GObject - see docs http://library.isr.ist.utl.pt/docs/pygtk2reference/gobject-functions.html#function-gobject--timeout-add
        return True

    def _handlechangedvalue(self, path, value):
        logging.debug("someone else updated %s to %s" % (path, value))
        return True  # accept the change


def main():
    # configure logging
    logging.basicConfig(
        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
        handlers=[
            logging.FileHandler(
                "%s/current.log" % (os.path.dirname(os.path.realpath(__file__)))
            ),
            logging.StreamHandler(),
        ],
    )

    try:
        logging.info("Start")

        from dbus.mainloop.glib import DBusGMainLoop

        # Have a mainloop, so we can send/receive asynchronous calls to and from dbus
        DBusGMainLoop(set_as_default=True)

        # formatting
        _kwh = lambda p, v: (str(round(v, 2)) + "kWh")
        _a = lambda p, v: (str(round(v, 1)) + "A")
        _w = lambda p, v: (str(round(v, 1)) + "W")
        _v = lambda p, v: (str(round(v, 1)) + "V")

        # start our main-service
        pvac_output = DbusMqttInverterService(
            servicename="com.victronenergy.pvinverter",
            paths={
                "/Ac/Energy/Forward": {
                    "initial": None,
                    "textformat": _kwh,
                },  # energy produced by pv inverter
                "/Ac/Power": {"initial": 0, "textformat": _w},
                "/Ac/Current": {"initial": 0, "textformat": _a},
                "/Ac/Voltage": {"initial": 0, "textformat": _v},
                "/Ac/L1/Voltage": {"initial": 0, "textformat": _v},
                "/Ac/L2/Voltage": {"initial": 0, "textformat": _v},
                "/Ac/L3/Voltage": {"initial": 0, "textformat": _v},
                "/Ac/L1/Current": {"initial": 0, "textformat": _a},
                "/Ac/L2/Current": {"initial": 0, "textformat": _a},
                "/Ac/L3/Current": {"initial": 0, "textformat": _a},
                "/Ac/L1/Power": {"initial": 0, "textformat": _w},
                "/Ac/L2/Power": {"initial": 0, "textformat": _w},
                "/Ac/L3/Power": {"initial": 0, "textformat": _w},
                "/Ac/L1/Energy/Forward": {"initial": None, "textformat": _kwh},
                "/Ac/L2/Energy/Forward": {"initial": None, "textformat": _kwh},
                "/Ac/L3/Energy/Forward": {"initial": None, "textformat": _kwh},
            },
        )

        logging.info(
            "Connected to dbus, and switching over to gobject.MainLoop() (= event based)"
        )
        mainloop = gobject.MainLoop()
        mainloop.run()
    except Exception as e:
        logging.critical("Error at %s", "main", exc_info=e)


if __name__ == "__main__":
    main()
