"""See unit test function docstring."""

import json
import socket
import time
import threading
import mapreduce
import utils


def manager_message_generator(mock_sendall):
    """Fake Manager messages."""
    # First message
    #
    # Transfer control back to solution under test in between each check for
    # the register message to simulate the Worker calling recv() when there's
    # nothing to receive.
    for _ in utils.wait_for_register_messages(mock_sendall):
        yield None

    yield json.dumps({
        "message_type": "register_ack",
    }).encode("utf-8")
    yield None

    # Wait long enough for Worker to send two heartbeats
    time.sleep(1.5 * utils.TIME_BETWEEN_HEARTBEATS)

    # Shutdown
    yield json.dumps({
        "message_type": "shutdown",
    }).encode("utf-8")
    yield None


def test_heartbeat(mocker):
    """Verify Worker sends heartbeat messages to the Manager.

    Note: 'mocker' is a fixture function provided the the pytest-mock package.
    This fixture lets us override a library function with a temporary fake
    function that returns a hardcoded value while testing.

    See https://github.com/pytest-dev/pytest-mock/ for more info.
    """
    # Mock the socket library socket class
    mock_socket = mocker.patch("socket.socket")

    # sendall() records messages
    mock_sendall = mock_socket.return_value.__enter__.return_value.sendall

    # accept() returns a mock client socket
    mock_clientsocket = mocker.MagicMock()
    mock_accept = mock_socket.return_value.__enter__.return_value.accept
    mock_accept.return_value = (mock_clientsocket, ("127.0.0.1", 10000))

    # recv() returns values generated by manager_message_generator()
    mock_recv = mock_clientsocket.recv
    mock_recv.side_effect = manager_message_generator(mock_sendall)

    # Run student Worker code.  When student Worker calls recv(), it will
    # return the faked responses configured above.  When the student code calls
    # sys.exit(0), it triggers a SystemExit exception, which we'll catch.
    try:
        mapreduce.worker.Worker(
            host="localhost",
            port=6001,
            manager_host="localhost",
            manager_port=6000,
        )
        assert threading.active_count() == 1, "Failed to shutdown threads"
    except SystemExit as error:
        assert error.code == 0

    # Verify messages sent by the Worker, excluding heartbeat messages
    #
    # Pro-tip: show log messages and detailed diffs with
    #   $ pytest -vvs --log-cli-level=info tests/test_worker_X.py
    all_messages = utils.get_messages(mock_sendall)
    messages = utils.filter_not_heartbeat_messages(all_messages)
    assert messages == [
        {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        },
    ]

    # Verify UDP socket configuration.  This is the socket the Worker uses to
    # send heartbeat messages to the Manager.
    mock_socket.assert_has_calls([
        mocker.call(socket.AF_INET, socket.SOCK_DGRAM),
        mocker.call().__enter__().connect(("localhost", 6000)),
    ], any_order=True)

    # Verify heartbeat messages sent by the Worker
    heartbeat_messages = utils.filter_heartbeat_messages(all_messages)
    for heartbeat_message in heartbeat_messages:
        assert heartbeat_message == {
            "message_type": "heartbeat",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
    assert 2 <= len(heartbeat_messages) < 4
