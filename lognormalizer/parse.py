#!/usr/bin/env python3
"""
Postfix log parser
"""

import sys
import datetime
import traceback
from email.header import decode_header
from collections import defaultdict
from typing import Any, Dict, List
from itertools import tee


def init_clickhouse():
    """Init clickhouse client

    Returns: clickhouse client
    """
    import clickhouse_connect  # pylint: disable=import-outside-toplevel,import-error # pyright: ignore

    client = clickhouse_connect.get_client(
        host="localhost",
        username="default",
        password="",
    )
    client.command(
        "CREATE TABLE IF NOT EXISTS \
                events (\
                  timestamp DateTime,\
                  queue_id String,\
                  domain_from String,\
                  domains_to Array(String),\
                  message_from String,\
                  message_to Array(String),\
                  message_id String,\
                  message_subject Nullable(String),\
                  status String,\
                  status_code Int32,\
                  status_postfix_code String,\
                  status_description String,\
                  client Nullable(String),\
                  orig_to Nullable(String),\
                  dovecot_fileinto_action Nullable(String),\
                  delay Nullable(String),\
                  delays Nullable(String),\
                  dsn Nullable(String),\
                  nrcpt Nullable(String),\
                  relay Nullable(String),\
                  size Nullable(String),\
                  raw_log Array(String)\
                ) \
        ENGINE MergeTree ORDER BY timestamp"
    )

    return client


def pairwise(iterable):
    """s -> (s0,s1), (s1,s2), (s2, s3), ..."""
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


SECONDS_PER_MIN = 60


def now() -> datetime.datetime:
    """Returns local timestamp

    Returns: now with local timezone
    """
    return datetime.datetime.now(datetime.timezone.utc).astimezone()


OLD_LOGS = datetime.timedelta(minutes=10)


class PostfixEvent:  # pylint: disable=too-many-instance-attributes
    """Postfix event description

    Attributes:
        timestamp: time of first appearance of this queue id
        queue_id: queue id in postfix
        message_from: message from
        message_to: array of recipients
        message_id: message-id attribute
        status: text representation of the status code
        status_code: status code
        status_postfix_code: status code on postfix terminology (2.0.0 for example)
        status_description: full status description
        client: client ident
        orig_to: orig_to
        delay: delay
        delays: delays array
        dsn: dsn number
        nrcpt: nrcpt num
        relay: relay
        size: size
        uid: uid
    """

    timestamp: datetime.datetime
    queue_id: str
    domain_from: str
    domains_to: List[str]
    message_from: str
    message_to: List[str]
    message_id: str
    message_subject: str | None
    status: str
    status_code: int
    status_postfix_code: str
    status_description: str

    raw_log: List[str]

    dovecot_fileinto_action: str | None
    client: str | None
    orig_to: str | None
    delay: str | None
    delays: str | None
    dsn: str | None
    nrcpt: str | None
    relay: str | None
    size: str | None

    def _get_domain(self, mail: str) -> str:
        tokens = mail.split("@", 1)

        if len(tokens) == 2:
            return tokens[1]
        return mail

    def __init__(self, queue_id: str, obj: Dict[str, Any]):
        """Init PostfixEvent

        Args:
            dict: dict for parse
        """
        # requred
        self.timestamp = obj["timestamp"]
        self.queue_id = queue_id
        self.message_from = obj["from"].strip("<>")
        self.message_to = [mail.strip("<>") for mail in obj["to"]]
        self.message_id = obj["message-id"].strip("<>")
        self.status = obj["status"]
        self.status_description = obj["status_description"]
        self.raw_log = obj["raw_log"]

        self.domain_from = self._get_domain(self.message_from)
        self.domains_to = list(
            dict.fromkeys([self._get_domain(val) for val in self.message_to])
        )

        self.parse_status_code()

        self.dovecot_fileinto_action = obj.get("dovecot_fileinto_action")
        self.message_subject = obj.get("subject")
        self.client = obj.get("client")
        self.orig_to = obj.get("orig_to")
        self.delay = obj.get("delay")
        self.delays = obj.get("delays")
        self.dsn = obj.get("dsn")
        self.nrcpt = obj.get("nrcpt")
        self.relay = obj.get("relay")
        self.size = obj.get("size")

    def is_status_code(self, status: str) -> bool:
        """Check that string is status code
        >>> PostfixEvent().is_status_code("200")
        True
        >>> PostfixEvent().is_status_code("2000")
        False
        >>> PostfixEvent().is_status_code("")
        False

        Args:
            status: possible status code

        Returns: True if it real status code
        """
        return len(status) == 3 and all(c.isdigit() for c in status)

    def is_postfix_status_code(self, status: str) -> bool:
        """CHeck that string is postfix status code

        >>> PostfixEvent().is_postfix_status_code("2.0.0")
        True
        >>> PostfixEvent().is_postfix_status_code("2.5.0")
        True
        >>> PostfixEvent().is_postfix_status_code("250")
        False
        >>> PostfixEvent().is_postfix_status_code("a.a.a")
        False

        Args:
            status: possible status code

        Returns: True if it real status code
        """
        tokens = status.split(".")
        return len(tokens) == 3 and all(c.isdigit() for c in tokens)

    def parse_status_code(self):
        """Parse status code from description"""
        for status, postfix in pairwise(self.status_description.split()):
            if not self.is_status_code(status) or not self.is_postfix_status_code(
                postfix
            ):
                continue

            self.status_code = int(status)
            self.status_postfix_code = postfix


class PostfixLogParser:
    """Postfix log parser

    Attributes:
        state: local parser state
        next_cleanup: time for next cleanup
        cleanup_interval: cleanup interval
    """

    state: Dict[str, Dict[str, Any]]
    next_cleanup: datetime.datetime
    cleanup_interval: datetime.timedelta

    def __init__(self):
        """Init parser"""
        self.state = defaultdict(lambda: {})
        self.cleanup_interval = datetime.timedelta(minutes=1)
        self.next_cleanup = now() + self.cleanup_interval

    def _is_queue_id(self, maybe_queue_id: str) -> bool:
        """Checks that token is queue id
        >>> PostfixLogParser().is_queue_id("0A3F51021C3:")
        True
        >>> PostfixLogParser().is_queue_id("")
        False
        >>> PostfixLogParser().is_queue_id("0A3F51021C3")
        False

        Args:
            maybe_queue_id: token for potential queue id

        Returns: True if it real queue id
        """
        if len(maybe_queue_id) != 11 + 1:
            return False

        maybe_queue_id = maybe_queue_id[:-1]

        return all(c in "0123456789ABCDEF" for c in maybe_queue_id)

    def on_event(self, event: PostfixEvent):
        """On Event function

        Args:
            event: postfix event to send
        """

    def on_fault(self, s: str, e: Exception):
        """On Fault function

        Args:
            e: exception
        """

    def _cleanup_old_entities(self):
        """Function to cleanup old state entities"""
        ts = now()

        if self.next_cleanup > ts:
            return

        self.next_cleanup = ts + self.cleanup_interval
        self.state = {
            key: val
            for key, val in self.state.items()
            if ts - val["timestamp"] < OLD_LOGS
        }

    def _set_fields(self, entry: Dict[str, Any], tokens: List[str]):
        """Set fields for queue id"""
        idx = 0
        status_idx = None

        for token in tokens:
            if "=" in token:
                name, value = token.split("=", 1)
                value = value.rstrip(",")

                if name == "status":
                    status_idx = idx

                if name == "to":
                    entry[name] = entry.get(name, []) + [value]
                else:
                    entry[name] = value
            idx = idx + 1

        if status_idx:
            entry["status_description"] = (
                " ".join(tokens[status_idx + 1 :]).lstrip("(").rstrip(")")
            )

    def _try_to_parse_subject(self, queue_id: str, tokens: List[str]) -> bool:
        """Try to parse email subject

        Args:
            queue_id: queue id
            tokens: list of parsed log tokens

        Returns: True if subject parsed
        """
        if (
            tokens[0] == "warning:"
            and tokens[1] == "header"
            and tokens[2] == "Subject:"
        ):
            if tokens[3].startswith("=?UTF-8"):
                subject, encoding = decode_header(tokens[3])[0]
                self.state[queue_id]["subject"] = subject.decode(encoding)
            else:
                subject = []
                for token1, token2 in pairwise(tokens[3:]):
                    if token1 == "from" and token2.endswith(";"):
                        break

                    subject.append(token1)

                self.state[queue_id]["subject"] = " ".join(subject)

            return True

        return False

    def _handle_special_postfix_cases(self, queue_id: str, tokens: List[str]) -> bool:
        """Handle special parse cases

        Args:
            queue_id: queue id
            entry: state entry
            tokens: list of parsed log tokens

        Returns: True if case handled
        """
        if tokens[0] == "removed":
            event = PostfixEvent(queue_id, self.state[queue_id])
            del self.state[queue_id]
            self.on_event(event)
            return True

        if self._try_to_parse_subject(queue_id, tokens):
            return True

        return False

    def _handle_dovecot_case(self, tokens: List[str]) -> bool:
        if (
            tokens[4] == "Info:"
            and tokens[5] == "sieve:"
            and tokens[6].startswith("msgid=")
            and tokens[7] == "fileinto"
            and tokens[8] == "action:"
        ):
            _, msgid = tokens[6].split("=", 1)
            msgid = msgid[:-1]

            for _, entry in self.state.items():
                if entry.get("message-id") == msgid:
                    entry["dovecot_fileinto_action"] = " ".join(tokens[9:])
            entry = self.state.items()

        return False

    def feed_line(self, s: str):
        """Feed line to parser
        Args:
            line: line to parse
        """
        self._cleanup_old_entities()

        try:
            tokens = s.split()

            if len(tokens) < 5:
                return

            if self._handle_dovecot_case(tokens):
                return

            # parse '2024-04-09T20:20:02.078412+03:00' to datatime
            try:
                timestamp = datetime.datetime.fromisoformat(tokens[0])
            except ValueError:
                return

            if not self._is_queue_id(tokens[3]):
                return

            queue_id = tokens[3][:-1]
            self.state.setdefault(queue_id, {})
            entry = self.state[queue_id]

            if "raw_log" not in entry:
                entry["raw_log"] = [s]
            else:
                entry["raw_log"].append(s)

            if "timestamp" not in entry:
                entry["timestamp"] = timestamp

            if self._handle_special_postfix_cases(queue_id, tokens[4:]):
                return

            self._set_fields(entry, tokens)

        except Exception as e:  # pylint: disable=broad-exception-caught
            self.on_fault(s, e)


class ClickHousePostfixLogParser(PostfixLogParser):
    """Clickhouse implementation of PostfixLogParser

    Attributes:
        client: clickhouse client
    """

    def __init__(self, client):
        super().__init__()
        self.client = client

    def on_fault(self, s: str, e: Exception):
        """On Fault function

        Args:
            e: exception
        """
        exc_info = sys.exc_info()
        traceback.print_exception(*exc_info)
        print("Fault string: ", s)

    def on_event(self, event: PostfixEvent):
        """On Event function

        Args:
            event: postfix event to send
        """
        self.client.insert(
            "events",
            [
                [
                    event.timestamp,
                    event.queue_id,
                    event.domain_from,
                    event.domains_to,
                    event.message_from,
                    event.message_to,
                    event.message_id,
                    event.message_subject,
                    event.status,
                    event.status_code,
                    event.status_postfix_code,
                    event.status_description,
                    event.client,
                    event.orig_to,
                    event.dovecot_fileinto_action,
                    event.delay,
                    event.delays,
                    event.dsn,
                    event.nrcpt,
                    event.relay,
                    event.size,
                    event.raw_log,
                ]
            ],
            column_names=[
                "timestamp",
                "queue_id",
                "domain_from",
                "domains_to",
                "message_from",
                "message_to",
                "message_id",
                "message_subject",
                "status",
                "status_code",
                "status_postfix_code",
                "status_description",
                "client",
                "orig_to",
                "dovecot_fileinto_action",
                "delay",
                "delays",
                "dsn",
                "nrcpt",
                "relay",
                "size",
                "raw_log",
            ],
        )


if __name__ == "__main__":
    import cProfile
    from pprint import pprint

    with cProfile.Profile() as pr:
        parser = ClickHousePostfixLogParser(init_clickhouse())

        for line in sys.stdin.readlines():
            parser.feed_line(line.strip())

        pprint(parser, indent=4)

        pr.dump_stats("./profile.prof")
