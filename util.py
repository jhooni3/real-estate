import requests

def send_msg_to_slack(text):
    url = "<hook_url>"
    payload = "{\n\t\"text\": \""+text+"\"}"
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
        'postman-token': "f0c31595-8d8f-3de1-fc1d-01ec4f93aecc"
        }

    response = requests.request("POST", url, data=payload, headers=headers)

    print(response.text)

