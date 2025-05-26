import httpx
import base64

def get_event_count():
    # RabbitMQ Management API credentials
    username = "0b89a58aafec2c9af020595581c3be3e6191848d9008d3fb88"
    password = "4ffacd37fa833950a7712d22741a46d28fd67b470f53a32c4c"
    vhost = "kat"
    queue_name = "octopoes"
    # Encode credentials for Basic Auth
    auth_string = f"{username}:{password}"
    auth_bytes = auth_string.encode('ascii')
    base64_bytes = base64.b64encode(auth_bytes)
    base64_auth = base64_bytes.decode('ascii')
    # Set up headers with authentication
    headers = {
        'Authorization': f'Basic {base64_auth}'
    }
    # URL for the queue (URL-encode the vhost)
    url = f"http://localhost:15672/api/queues/{vhost}/{queue_name}"
    try:
        # Make the request
        with httpx.Client() as client:
            response = client.get(url, headers=headers)
        # Check if request was successful
        if response.status_code == 200:
            queue_info = response.json()
            return queue_info.get('messages', 0)
        else:
            return -1  # Error code
    except Exception:
        return -1  # Error code

if __name__ == "__main__":
    count = get_event_count()
    print(f"Number of events in queue: {count}")
