import requests

def send_msg_to_slack(text):
    url = "https://hooks.slack.com/services/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"
    payload = "{\n\t\"text\": \""+text+"\"}"
    headers = {
        'content-type': "application/json",
        'cache-control': "no-cache",
        'postman-token': "f0c31595-8d8f-3de1-fc1d-01ec4f93aecc"
        }

    response = requests.request("POST", url, data=payload, headers=headers)

    print(response.text)

