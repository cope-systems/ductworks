from unittest import TestCase
from assertpy import assert_that
import threading
import os
import time

from ductworks.message_duct import MessageDuctParent, MessageDuctChild, create_psuedo_anonymous_duct_pair


class MessageDuctIntegrationTest(TestCase):
    def test_basic_message_passing(self):
        test_data = ["hello world", 42]
        parent, child = create_psuedo_anonymous_duct_pair()
        bind_address = parent.bind_address
        parent.send(test_data)
        assert_that(child.recv()).is_equal_to(test_data)
        child.close()
        parent.close()
        assert_that(os.path.exists(bind_address)).is_false()

    def test_big_message(self):
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

    def test_performance(self):
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
        print "Ductwork approx perf: {} KB/s".format(ductwork_approx_perf)
        t.join()
