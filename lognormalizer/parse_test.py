#!/usr/bin/env python3
# pylint: skip-file

import datetime

from parse import PostfixLogParser, PostfixEvent, now
from typing import List, Tuple


class ExtPostfixLogParser(PostfixLogParser):
    events: List[PostfixEvent]
    faults: List[Tuple[str, Exception]]

    def __init__(self):
        super().__init__()
        self.events = []
        self.faults = []

    def on_event(self, event: PostfixEvent):
        self.events.append(event)

    def on_fault(self, s: str, e: Exception):
        self.faults.append((s, e))


def test_basic():
    parser = ExtPostfixLogParser()

    test_input = """
2024-04-09T20:22:42.548478+03:00 localhost postfix/submission/smtpd[2182082]: 85C6C10208A: client=mail.localhost[192.168.0.1], sasl_method=PLAIN, sasl_username=user@localhost
2024-04-09T20:22:42.596265+03:00 localhost postfix/cleanup[2182089]: 85C6C10208A: message-id=<214b89-66157980-3-dc1e520@120925011>
2024-04-09T20:22:42.736487+03:00 localhost postfix/qmgr[837]: 85C6C10208A: from=<user@localhost>, size=952, nrcpt=1 (queue active)
2024-04-09T20:22:42.736796+03:00 localhost postfix/submission/smtpd[2182082]: disconnect from mail.localhost[192.168.0.1] ehlo=2 starttls=1 auth=1 mail=1 rcpt=1 data=1 quit=1 commands=8
2024-04-09T20:22:43.039303+03:00 localhost postfix/lmtp[2182091]: 85C6C10208A: to=<user@localhost>, relay=mail.localhost[private/dovecot-lmtp], delay=0.55, delays=0.25/0.02/0.02/0.27, dsn=2.0.0, status=sent (250 2.0.0 <user@localhost> VB7QLWJ5FWbMSyEA0J78UA Saved)
2024-04-09T20:22:43.039628+03:00 localhost postfix/qmgr[837]: 85C6C10208A: removed
"""
    for line in test_input.splitlines():
        parser.feed_line(line)

    assert len(parser.faults) == 0
    assert len(parser.events) == 1
    assert parser.events[0].queue_id == "85C6C10208A"
    assert parser.events[0].message_from == "user@localhost"
    assert parser.events[0].message_to == ["user@localhost"]
    assert parser.events[0].message_id == "214b89-66157980-3-dc1e520@120925011"
    assert parser.events[0].status == "sent"
    assert parser.events[0].status_code == 250
    assert parser.events[0].status_postfix_code == "2.0.0"
    assert (
        parser.events[0].status_description
        == "250 2.0.0 <user@localhost> VB7QLWJ5FWbMSyEA0J78UA Saved"
    )
    assert parser.events[0].client == "mail.localhost[192.168.0.1]"
    assert parser.events[0].raw_log == [
        "2024-04-09T20:22:42.548478+03:00 localhost postfix/submission/smtpd[2182082]: 85C6C10208A: client=mail.localhost[192.168.0.1], sasl_method=PLAIN, sasl_username=user@localhost",
        "2024-04-09T20:22:42.596265+03:00 localhost postfix/cleanup[2182089]: 85C6C10208A: message-id=<214b89-66157980-3-dc1e520@120925011>",
        "2024-04-09T20:22:42.736487+03:00 localhost postfix/qmgr[837]: 85C6C10208A: from=<user@localhost>, size=952, nrcpt=1 (queue active)",
        "2024-04-09T20:22:43.039303+03:00 localhost postfix/lmtp[2182091]: 85C6C10208A: to=<user@localhost>, relay=mail.localhost[private/dovecot-lmtp], delay=0.55, delays=0.25/0.02/0.02/0.27, dsn=2.0.0, status=sent (250 2.0.0 <user@localhost> VB7QLWJ5FWbMSyEA0J78UA Saved)",
        "2024-04-09T20:22:43.039628+03:00 localhost postfix/qmgr[837]: 85C6C10208A: removed",
    ]


def test_cleanup():
    parser = ExtPostfixLogParser()

    test_input = """
2024-04-09T20:22:42.548478+03:00 localhost postfix/submission/smtpd[2182082]: 85C6C10208A: client=mail.localhost[192.168.0.1], sasl_method=PLAIN, sasl_username=user@localhost
2024-04-09T20:22:42.596265+03:00 localhost postfix/cleanup[2182089]: 85C6C10208A: message-id=<214b89-66157980-3-dc1e520@120925011>
2024-04-09T20:22:42.736487+03:00 localhost postfix/qmgr[837]: 85C6C10208A: from=<user@localhost>, size=952, nrcpt=1 (queue active)
2024-04-09T20:22:42.736796+03:00 localhost postfix/submission/smtpd[2182082]: disconnect from mail.localhost[192.168.0.1] ehlo=2 starttls=1 auth=1 mail=1 rcpt=1 data=1 quit=1 commands=8
2024-04-09T20:22:43.039303+03:00 localhost postfix/lmtp[2182091]: 85C6C10208A: to=<user@localhost>, relay=mail.localhost[private/dovecot-lmtp], delay=0.55, delays=0.25/0.02/0.02/0.27, dsn=2.0.0, status=sent (250 2.0.0 <user@localhost> VB7QLWJ5FWbMSyEA0J78UA Saved)
"""
    for line in test_input.splitlines():
        parser.feed_line(line)

    assert len(parser.faults) == 0
    assert len(parser.events) == 0
    assert len(parser.state) == 1
    assert parser.state.get("85C6C10208A") is not None

    parser.state["85C6C10208A"]["timestamp"] = now()
    parser.next_cleanup = now()

    parser.feed_line("LINE")

    assert len(parser.faults) == 0
    assert len(parser.events) == 0
    assert len(parser.state) == 1
    assert parser.state.get("85C6C10208A") is not None

    parser.state["85C6C10208A"]["timestamp"] = now() - datetime.timedelta(days=1)
    parser.next_cleanup = now()

    parser.feed_line("LINE")

    assert len(parser.faults) == 0
    assert len(parser.state) == 0
    assert parser.state.get("85C6C10208A") is None


def test_mixed():
    parser = ExtPostfixLogParser()

    test_input = """
2024-04-09T20:22:42.305279+03:00 localhost postfix/submission/smtpd[2182082]: connect from mail.localhost[192.168.0.1]
2024-04-09T20:22:42.548478+03:00 localhost postfix/submission/smtpd[2182082]: 85C6C10208A: client=mail.localhost[192.168.0.1], sasl_method=PLAIN, sasl_username=user@localhost
2024-04-09T20:25:02.187267+03:00 localhost postfix/pickup[2181174]: 2D7291020EE: uid=0 from=<root>
2024-04-09T20:22:42.596265+03:00 localhost postfix/cleanup[2182089]: 85C6C10208A: message-id=<214b89-66157980-3-dc1e520@120925011>
2024-04-09T20:25:02.207945+03:00 localhost postfix/cleanup[2182115]: 2D7291020EE: message-id=<20240409172502.2D7291020EE@mail.localhost>
2024-04-09T20:22:42.736487+03:00 localhost postfix/qmgr[837]: 85C6C10208A: from=<user@localhost>, size=952, nrcpt=1 (queue active)
2024-04-09T20:25:02.483114+03:00 localhost postfix/qmgr[837]: 2D7291020EE: from=<root@localhost>, size=732, nrcpt=1 (queue active)
2024-04-09T20:22:42.736796+03:00 localhost postfix/submission/smtpd[2182082]: disconnect from mail.localhost[192.168.0.1] ehlo=2 starttls=1 auth=1 mail=1 rcpt=1 data=1 quit=1 commands=8
2024-04-09T20:25:02.134775+03:00 localhost fetchmail-all[2182112]: Please create: /var/lock/fetchmail
"""
    for line in test_input.splitlines():
        parser.feed_line(line)

    assert len(parser.faults) == 0
    assert len(parser.events) == 0
    assert len(parser.state) == 2

    parser.feed_line(
        "2024-04-09T20:22:43.039303+03:00 localhost postfix/lmtp[2182091]: 85C6C10208A: to=<user@localhost>, relay=mail.localhost[private/dovecot-lmtp], delay=0.55, delays=0.25/0.02/0.02/0.27, dsn=2.0.0, status=sent (250 2.0.0 <user@localhost> VB7QLWJ5FWbMSyEA0J78UA Saved)"
    )
    parser.feed_line(
        "2024-04-09T20:22:43.039628+03:00 localhost postfix/qmgr[837]: 85C6C10208A: removed"
    )

    assert len(parser.faults) == 0
    assert len(parser.events) == 1
    assert len(parser.state) == 1
    assert parser.events[0].message_from == "user@localhost"
    assert parser.events[0].message_to == ["user@localhost"]
    assert parser.events[0].message_id == "214b89-66157980-3-dc1e520@120925011"
    assert parser.events[0].status == "sent"
    assert parser.events[0].status_code == 250
    assert parser.events[0].status_postfix_code == "2.0.0"
    assert (
        parser.events[0].status_description
        == "250 2.0.0 <user@localhost> VB7QLWJ5FWbMSyEA0J78UA Saved"
    )

    assert parser.events[0].client == "mail.localhost[192.168.0.1]"

    parser.feed_line(
        """2024-04-09T20:25:02.558830+03:00 localhost postfix/lmtp[2182117]: 2D7291020EE: to=<root@localhost>, orig_to=<root>, relay=mail.localhost[private/dovecot-lmtp], delay=0.4, delays=0.33/0.02/0.02/0.04, dsn=5.1.1, status=bounced (host mail.localhost[private/dovecot-lmtp] said: 550 5.1.1 <root@localhost> User doesn't exist: root@localhost (in reply to RCPT TO command))"""
    )
    parser.feed_line(
        "2024-04-09T20:25:02.570564+03:00 localhost postfix/qmgr[837]: 2D7291020EE: removed"
    )
    assert len(parser.faults) == 0
    assert len(parser.events) == 2
    assert len(parser.state) == 0

    assert parser.events[1].message_from == "root@localhost"
    assert parser.events[1].message_to == ["root@localhost"]
    assert parser.events[1].message_id == "20240409172502.2D7291020EE@mail.localhost"
    assert parser.events[1].status == "bounced"
    assert parser.events[1].status_code == 550
    assert parser.events[1].status_postfix_code == "5.1.1"


def test_parse_subject():
    parser = ExtPostfixLogParser()
    test_input = """
2024-05-10T14:21:02.940024+03:00 localhost postfix/smtpd[96814]: E54DD102A90: client=mail-lf1-f47.google.com[209.85.167.47]
2024-05-10T14:21:02.943156+03:00 localhost postfix/cleanup[96830]: E54DD102A90: message-id=<c6817a87-5fd4-4702-a784-ad400bbd9641@gmail.com>
2024-05-10T14:21:02.943386+03:00 localhost postfix/cleanup[96830]: E54DD102A90: warning: header Subject: =?UTF-8?B?dGVzdCAtINCi0LXRgdGC?= from mail-lf1-f47.google.com[209.85.167.47]; from=<example@gmail.com> to=<max1@localhost> proto=ESMTP helo=<mail-lf1-f47.google.com>
2024-05-10T14:21:03.771707+03:00 localhost postfix/qmgr[96647]: E54DD102A90: from=<example@gmail.com>, size=3089, nrcpt=1 (queue active)
2024-05-10T14:21:03.797619+03:00 localhost postfix/smtpd[96814]: disconnect from mail-lf1-f47.google.com[209.85.167.47] ehlo=2 starttls=1 mail=1 rcpt=1 bdat=1 quit=1 commands=7
2024-05-10T14:21:04.415129+03:00 localhost postfix/lmtp[96831]: E54DD102A90: to=<max1@localhost>, relay=mail.localhost[private/dovecot-lmtp], delay=1.5, delays=0.89/0.02/0.02/0.61, dsn=2.0.0, status=sent (250 2.0.0 <max1@localhost> 8lAxMB8DPmZAegEA0J78UA Saved)
2024-05-10T14:21:04.415641+03:00 localhost postfix/qmgr[96647]: E54DD102A90: removed
"""

    for line in test_input.splitlines():
        parser.feed_line(line)

    assert len(parser.faults) == 0
    assert len(parser.events) == 1

    assert parser.events[0].message_from == "example@gmail.com"
    assert parser.events[0].message_to == ["max1@localhost"]
    assert (
        parser.events[0].message_id == "c6817a87-5fd4-4702-a784-ad400bbd9641@gmail.com"
    )
    assert parser.events[0].message_subject == "test - Тест"
    assert parser.events[0].status == "sent"
    assert parser.events[0].status_code == 250
    assert parser.events[0].status_postfix_code == "2.0.0"
    assert (
        parser.events[0].status_description
        == "250 2.0.0 <max1@localhost> 8lAxMB8DPmZAegEA0J78UA Saved"
    )


def test_parse_ascii_subject():
    parser = ExtPostfixLogParser()

    test_input = """
2024-05-12T00:05:01.479974+03:00 localhost postfix/pickup[12895]: 74F8A1019D1: uid=0 from=<root>
2024-05-12T00:05:01.505890+03:00 localhost postfix/cleanup[13779]: 74F8A1019D1: warning: header Subject: Cron <root@test3-stack> /usr/bin/sudo -H -u vmail /var/www/postfixadmin/ADDITIONS/fetchmail.pl from local; from=<root@localhost> to=<root@localhost>
2024-05-12T00:05:01.506337+03:00 localhost postfix/cleanup[13779]: 74F8A1019D1: message-id=<20240511210501.74F8A1019D1@mail.localhost>
2024-05-12T00:05:01.565946+03:00 localhost postfix/qmgr[7094]: 74F8A1019D1: from=<root@localhost>, size=732, nrcpt=1 (queue active)
2024-05-12T00:05:01.657364+03:00 localhost postfix/lmtp[13781]: 74F8A1019D1: to=<root@localhost>, orig_to=<root>, relay=mail.localhost[private/dovecot-lmtp], delay=0.21, delays=0.12/0.02/0.02/0.05, dsn=5.1.1, status=bounced (host mail.localhost[private/dovecot-lmtp] said: 550 5.1.1 <root@localhost> User doesn't exist: root@localhost (in reply to RCPT TO command))
2024-05-12T00:05:01.663062+03:00 localhost postfix/bounce[13785]: 74F8A1019D1: sender non-delivery notification: A08BD101F4A
2024-05-12T00:05:01.663913+03:00 localhost postfix/qmgr[7094]: 74F8A1019D1: removed
"""
    for line in test_input.splitlines():
        parser.feed_line(line)

    assert len(parser.faults) == 0
    assert len(parser.events) == 1

    assert parser.events[0].message_from == "root@localhost"
    assert parser.events[0].message_to == ["root@localhost"]
    assert (
        parser.events[0].message_subject
        == "Cron <root@test3-stack> /usr/bin/sudo -H -u vmail /var/www/postfixadmin/ADDITIONS/fetchmail.pl"
    )
    assert parser.events[0].status_code == 550


def test_dovecot_integration():
    parser = ExtPostfixLogParser()

    test_input = """
2024-04-30T15:09:26.900515+03:00 localhost postfix/smtpd[2799418]: DBC88100165: client=unknown[91.215.169.237]
2024-04-30T15:09:27.061817+03:00 localhost postfix/cleanup[2799490]: DBC88100165: message-id=<2cd16c8571abc90a2986ef44b051ebe1d44c9c29@vrufa.ru>
2024-04-30T15:09:27.357334+03:00 localhost postfix/qmgr[837]: DBC88100165: from=<mishin@vrufa.ru>, size=3132, nrcpt=1 (queue active)
2024-04-30T15:09:27.416842+03:00 localhost postfix/smtpd[2799418]: disconnect from unknown[91.215.169.237] ehlo=2 starttls=1 mail=1 rcpt=1 data=1 quit=1 commands=7
2024-04-30T15:09:27.510034+03:00 localhost postfix/lmtp[2799491]: DBC88100165: to=<admin@localhost>, relay=mail.localhost[private/dovecot-lmtp], delay=0.71, delays=0.56/0.01/0.02/0.12, dsn=2.0.0, status=sent (250 2.0.0 <admin@localhost> N2hLF3ffMGaEtyoA0J78UA Saved)
Apr 30 15:09:27 lmtp(admin@localhost)<2799492><N2hLF3ffMGaEtyoA0J78UA>: Info: sieve: msgid=<2cd16c8571abc90a2986ef44b051ebe1d44c9c29@vrufa.ru>: fileinto action: stored mail into mailbox 'Junk'
2024-04-30T15:09:27.510457+03:00 localhost postfix/qmgr[837]: DBC88100165: removed
"""
    for line in test_input.splitlines():
        parser.feed_line(line)

    assert len(parser.faults) == 0
    assert len(parser.events) == 1

    assert parser.events[0].message_from == "mishin@vrufa.ru"
    assert parser.events[0].message_to == ["admin@localhost"]
    assert (
        parser.events[0].message_id
        == "2cd16c8571abc90a2986ef44b051ebe1d44c9c29@vrufa.ru"
    )
    assert parser.events[0].status == "sent"
    assert parser.events[0].dovecot_fileinto_action == "stored mail into mailbox 'Junk'"


def test_multiple_to():
    parser = ExtPostfixLogParser()

    test_input = """
2024-05-11T19:19:29.118301+03:00 test3-stack postfix/smtpd[7113]: 1CC191002F0: client=unknown[209.85.208.172]
2024-05-11T19:19:29.128386+03:00 test3-stack postfix/cleanup[7121]: 1CC191002F0: message-id=<1f9d20ee-0fb7-430f-b8ca-fa8b78e7a333@gmail.com>
2024-05-11T19:19:29.128767+03:00 test3-stack postfix/cleanup[7121]: 1CC191002F0: warning: header Subject: =?UTF-8?B?dGVzdDIgLSDQotC10YHRgjI=?= from unknown[209.85.208.172]; from=<example@gmail.com> to=<max2@localhost> proto=ESMTP helo=<mail-lj1-f172.google.com>
2024-05-11T19:19:29.252088+03:00 test3-stack postfix/qmgr[7094]: 1CC191002F0: from=<example@gmail.com>, size=3140, nrcpt=2 (queue active)
2024-05-11T19:19:30.108425+03:00 test3-stack postfix/lmtp[7122]: 1CC191002F0: to=<max1@localhost>, relay=mail.localhost[private/dovecot-lmtp], delay=1.1, delays=0.25/0.02/0.03/0.81, dsn=2.0.0, status=sent (250 2.0.0 <max1@localhost> Ck2hEZGaP2bTGwAA0J78UA Saved)
2024-05-11T19:19:30.109424+03:00 test3-stack postfix/lmtp[7122]: 1CC191002F0: to=<max2@localhost>, relay=mail.localhost[private/dovecot-lmtp], delay=1.1, delays=0.25/0.02/0.03/0.81, dsn=2.0.0, status=sent (250 2.0.0 <max2@localhost> Ck2hEZGaP2bTGwAA0J78UA:R2 Saved)
2024-05-11T19:19:30.109590+03:00 test3-stack postfix/qmgr[7094]: 1CC191002F0: removed
"""
    for line in test_input.splitlines():
        parser.feed_line(line)

    assert len(parser.faults) == 0
    assert len(parser.events) == 1

    assert parser.events[0].message_from == "example@gmail.com"
    assert parser.events[0].message_subject == "test2 - Тест2"
    assert parser.events[0].domain_from == "gmail.com"
    assert parser.events[0].domains_to == ["localhost"]
    assert parser.events[0].message_to == [
        "max1@localhost",
        "max2@localhost",
    ]
