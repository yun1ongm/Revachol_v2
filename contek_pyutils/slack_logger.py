import logging
import re
import sys
import time

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class SlackHandler(logging.Handler):
    def __init__(
        self,
        token: str,
        destination: str,
        limit=20,
    ):
        logging.Handler.__init__(self)
        self.client = WebClient(token=token)
        self.limit = limit  # max number of messages in the queue
        self.msg_queue = []
        self.last_post_time = 0
        self.slack_rate_limit = 1.0

        if re.fullmatch(r"^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w+$", destination):
            # Lookup user ID by email
            self.channel_id = self.client.users_lookupByEmail(email=destination)["user"]["id"]
        else:
            # Lookup channel ID by name
            channel_info_list = (
                self.client.conversations_list(types="private_channel")["channels"]
                + self.client.conversations_list(types="public_channel")["channels"]
            )
            self.channel_id = next(filter(lambda x: x["name"] == destination, channel_info_list))["id"]

    def emit(self, record):
        log_entry = self.format(record)
        self.msg_queue.append(log_entry)
        if time.time() - self.last_post_time >= self.slack_rate_limit or len(self.msg_queue) >= self.limit:
            self.post_to_slack(self.msg_queue)
            self.msg_queue = []
            self.last_post_time = time.time()

    def post_to_slack(self, msg_queue):
        try:
            # Create blocks of text with markdown
            blocks = [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"```\n{msg}\n```"},
                }
                for msg in msg_queue
            ]
            response = self.client.chat_postMessage(channel=self.channel_id, blocks=blocks, text=msg_queue[0])
            if not response["ok"]:
                print(f"Got an error: {response['error']}", file=sys.stderr)
        except SlackApiError as e:
            print(f"Got an error: {e.response['error']}", file=sys.stderr)
        except Exception as e:
            print(f"Got an error: {e}", file=sys.stderr)


if __name__ == "__main__":

    def test():
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)

        # replace these with your actual Slack App token and user email
        token = "xoxb-894875048401-6218309154503-4vEdcDtiGCGJUcfY56x6mJWn"
        # email = "panyue@contek.io"

        sh = SlackHandler(token=token, destination="alert_offline_data_service")
        sh.setLevel(logging.INFO)

        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        sh.setFormatter(formatter)

        logger.addHandler(sh)
        for i in range(10):
            logger.info("Hello, Slack!")
        for i in range(5):
            time.sleep(1)
            logger.info("Hello, Slack!")

    test()
