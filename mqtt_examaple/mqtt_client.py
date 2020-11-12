import csv
import json
import datetime
import time

from cloudiot_mqtt_example import get_client, parse_command_line_args

def send_iot_device_messages(args):
    """Connects a device, sends data, and receives data."""
    # [START iot_mqtt_run]
    global minimum_backoff_time
    global MAXIMUM_BACKOFF_TIME

    # Publish to the events or state topic based on the flag.
    sub_topic = 'events' if args.message_type == 'event' else 'state'

    mqtt_topic = '/devices/{}/{}'.format(args.device_id, sub_topic)

    jwt_iat = datetime.datetime.utcnow()
    jwt_exp_mins = args.jwt_expires_minutes
    client = get_client(
        args.project_id, args.cloud_region, args.registry_id,
        args.device_id, args.private_key_file, args.algorithm,
        args.ca_certs, args.mqtt_bridge_hostname, args.mqtt_bridge_port)

    # Publish num_messages messages to the MQTT bridge once per second.
    with open('data\iot_waste_tracking.tsv', newline='') as tsvfile:
        file_rec = csv.DictReader(tsvfile, delimiter='\t')
        # file_rec = csv.reader(tsvfile, delimiter='\t')
        for row in file_rec:

            payload = json.dumps(row)
            print(payload)

            # Process network events.
            client.loop()

            # [START iot_mqtt_jwt_refresh]
            seconds_since_issue = (datetime.datetime.utcnow() - jwt_iat).seconds
            if seconds_since_issue > 60 * jwt_exp_mins:
                print('Refreshing token after {}s'.format(seconds_since_issue))
                jwt_iat = datetime.datetime.utcnow()
                client.loop()
                client.disconnect()
                client = get_client(
                    args.project_id, args.cloud_region,
                    args.registry_id, args.device_id, args.private_key_file,
                    args.algorithm, args.ca_certs, args.mqtt_bridge_hostname,
                    args.mqtt_bridge_port)
            # [END iot_mqtt_jwt_refresh]
            # Publish "payload" to the MQTT topic. qos=1 means at least once
            # delivery. Cloud IoT Core also supports qos=0 for at most once
            # delivery.
            client.publish(mqtt_topic, payload, qos=1)

            # Send events every second. State should not be updated as often
            for i in range(0, 2):
                time.sleep(1)
                client.loop()


def main():
    args = parse_command_line_args()
    send_iot_device_messages(args)
    print('Finished.')

if __name__ == '__main__':
    main()
