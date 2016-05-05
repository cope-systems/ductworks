from __future__ import print_function
from unittest import TestCase
from assertpy import assert_that
import threading
import os
import time
import subprocess
import sys

from ductworks.message_duct import MessageDuctParent, MessageDuctChild, create_psuedo_anonymous_duct_pair

from integration_tests import SUBPROCESS_TEST_SCRIPT, ROOT_DIR


class MessageDuctIntegrationTest(TestCase):
    def test_basic_message_passing(self):
        """
        As a Python developer,
        I want to be able to easily create a message duct pair and send structured data,
        and receive (and reassemble it correctly) so that I don't have to do lots of work and things "just work".
        """
        test_data = ["hello world", 42]
        parent, child = create_psuedo_anonymous_duct_pair()
        bind_address = parent.bind_address
        parent.send(test_data)
        assert_that(child.recv()).is_equal_to(test_data)
        child.close()
        parent.close()
        assert_that(os.path.exists(bind_address)).is_false()

    def test_tcp_message_passing(self):
        """
        As a Python developer,
        I want to also be able to send messages over TCP instead of Unix Domain sockets,
        so that I can make things work even when systems are remote or lack UDS facilities.
        """
        test_data = ["bob", "saget"]
        parent = MessageDuctParent.psuedo_anonymous_tcp_parent_duct()
        parent.bind()
        child = MessageDuctChild.psuedo_anonymous_tcp_child_duct(parent.listener_address[0], parent.listener_address[1])
        child.connect()
        parent.listen()
        parent.send(test_data)
        assert_that(child.recv()).is_equal_to(test_data)
        child.close()
        parent.close()

    def test_big_message(self):
        """
        As a Python developer,
        I want to be able to send and receive large messages and data structures faithfully,
        so that I don't have to worry about data get corrupted or lost when I send more data.
        """
        big_string = "lol" * 1024 * 128
        big_list = [big_string, 1, big_string, 2, big_string, 3]

        parent, child = create_psuedo_anonymous_duct_pair()
        bind_address = parent.bind_address

        def child_target():
            child.send(big_list)

        t = threading.Thread(target=child_target)
        t.start()

        assert_that(parent.recv()).is_equal_to(big_list)
        child.close()
        parent.close()
        assert_that(os.path.exists(bind_address)).is_false()

    def test_multiple_writers(self):
        """
        As a Python developer,
        I want to be able to specify a lock, and for multiple writers to be able to send data without race conditions,
        so that I can scale out nicely with concurrency without working too hard.
        """
        big_string = "lol" * 1024 * 128
        big_list = [big_string, 1, big_string, 2, big_string, 3]

        parent, child = create_psuedo_anonymous_duct_pair(child_lock=threading.Lock())
        bind_address = parent.bind_address

        def child_target():
            for _ in range(25):
                child.send(big_list)

        threads = []
        for _ in range(4):
            t = threading.Thread(target=child_target)
            t = t.start()
            threads.append(t)

        while parent.poll(10) is True:
            assert_that(parent.recv()).is_equal_to(big_list)
        child.close()
        parent.close()
        assert_that(os.path.exists(bind_address)).is_false()

    def test_ducts_with_subprocess(self):
        """
        As a Python developer,
        I want to be able to start a new Python interpreter (or anything else that supports the message duct protocol)
        in a child (or any other process) and be able to communicate with minimal overhead, so that I can
        support my often various concurrency and communication needs.
        """
        assert_that(SUBPROCESS_TEST_SCRIPT).exists()
        proc = None
        parent = None
        try:
            parent = MessageDuctParent.psuedo_anonymous_parent_duct()
            parent.bind()
            proc = subprocess.Popen(
                [sys.executable, SUBPROCESS_TEST_SCRIPT, parent.listener_address], env={'PYTHONPATH': ROOT_DIR}
            )
            parent.listen()
            for _ in range(100):
                parent.send("pingpong")
                parent.poll(1)
                assert_that(parent.recv()).is_equal_to("pingpong")
            parent.send(None)
            time.sleep(1)
        finally:
            if parent:
                parent.close()
            if proc:
                proc.terminate()

    def test_performance(self):
        """
        As a Python developer,
        I want my message duct to have reasonably good performance,
        so that the duct does not become a major bottleneck in my application.
        """
        big_string = u"\u0FF0lol" * 1024 * 128

        parent = MessageDuctParent.psuedo_anonymous_parent_duct()
        parent.bind()
        bind_address = parent.bind_address
        child = MessageDuctChild.psuedo_anonymous_child_duct(bind_address)
        child.connect()
        parent.listen()

        time.sleep(1)

        def child_target_duct():
            for _ in range(250):
                child.send(big_string)

        t = threading.Thread(target=child_target_duct)
        t.start()

        start_time = time.time()
        total_data = 0
        while parent.poll(0.01) is True:
            recv_data = parent.recv()
            assert_that(recv_data).is_equal_to(big_string)
            total_data += len(recv_data)
        ductwork_approx_perf = total_data/((time.time() - start_time)*1024)
        child.close()
        parent.close()
        assert_that(os.path.exists(bind_address)).is_false()
        print("Ductwork approx perf: {} KB/s".format(ductwork_approx_perf))
        t.join()
