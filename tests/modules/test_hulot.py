# coding: utf-8
import time
import pytest

import zmq

from oar.modules.hulot import (Hulot, HulotClient, fill_timeouts, get_timeout)
from oar.lib import (db, config)
from .fakezmq import FakeZmq

import oar.lib.tools
import oar.lib.tools as tools


# Set undefined config value to default one
DEFAULT_CONFIG = {
    'HULOT_SERVER': 'localhost',
    'HULOT_PORT' : 6670,
    'ENERGY_SAVING_WINDOW_SIZE': 25,
    'ENERGY_SAVING_WINDOW_TIME': 60,
    'ENERGY_SAVING_WINDOW_TIMEOUT': 120,
    'ENERGY_SAVING_NODE_MANAGER_WAKEUP_TIMEOUT': 900,
    'ENERGY_MAX_CYCLES_UNTIL_REFRESH': 5000,
    'OAR_RUNTIME_DIRECTORY': '/var/lib/oar',
    'ENERGY_SAVING_NODES_KEEPALIVE': "type='default':0",
    'ENERGY_SAVING_WINDOW_FORKER_BYPASS': 'yes',
    'ENERGY_SAVING_WINDOW_FORKER_SIZE': 20,
    'ENERGY_SAVING_NODE_MANAGER_WAKE_UP_CMD': 'wake_cmd',
    'ENERGY_SAVING_NODE_MANAGER_SLEEP_CMD': 'sleep_cmd'
}

called_command = ''
def fake_call(cmd, shell):
    global called_command
    called_command = cmd

@pytest.fixture(scope='function', autouse=True)
def monkeypatch_tools(request, monkeypatch):
    monkeypatch.setattr(zmq, 'Context', FakeZmq)
    monkeypatch.setattr(oar.lib.tools, 'call', fake_call)


@pytest.fixture(scope="function", autouse=True)
def setup(request):
    config.setdefault_config(DEFAULT_CONFIG)
    FakeZmq.reset()
    oar.lib.tools.zmq_context = None
    oar.lib.tools.almighty_socket = None
    oar.lib.tools.bipbip_commander_socket = None

    @request.addfinalizer
    def teardown():
        global called_command
        called_command = ''

@pytest.yield_fixture(scope='function')
def minimal_db_initialization(request):
    with db.session(ephemeral=True):
        # add some resources
        
        # available_upto=0 : to disable the wake-up and halt
        # available_upto=1 : to disable the wake-up (but not the halt)
        # available_upto=2147483647 : to disable the halt (but not the wake-up)
        # available_upto=2147483646 : to enable wake-up/halt forever
        # available_upto=<timestamp> : to enable the halt, and the wake-up until the date given by <timestamp>
        for i in range(2):
            db['Resource'].create(network_address="localhost0",available_upto=2147483646)
        for i in range(2):
            db['Resource'].create(network_address="localhost1",available_upto=2147483646)
        for i in range(2):
            db['Resource'].create(network_address="localhost2",
                                  state='Absent', available_upto=2147483646)
        for i in range(2):
            db['Resource'].create(network_address="localhost3",
                                  state='Absent', available_upto=2147483646)
        db.commit()
        yield

def test_fill_timeouts_1():
    timeouts = fill_timeouts("10")
    assert timeouts == {1: 10}

def test_fill_timeouts_2():
    timeouts = fill_timeouts("  1:500  11:1000 21:2000 ")
    assert timeouts == {1: 500, 11:1000, 21:2000}

def test_get_timeout(): 
    timeout = get_timeout({1: 500, 11:1000, 21:2000, 30:3000}, 15)
    assert timeout == 1000

def test_bad_energy_saving_nodes_keepalive_1():
    config['ENERGY_SAVING_NODES_KEEPALIVE'] = 'bad'
    hulot = Hulot()
    config['ENERGY_SAVING_NODES_KEEPALIVE'] = "type='default':0"
    assert hulot.exit_code == 3
    
def test_bad_energy_saving_nodes_keepalive_2():
    config['ENERGY_SAVING_NODES_KEEPALIVE'] = "type='default':3, bad:bad"
    hulot = Hulot()
    config['ENERGY_SAVING_NODES_KEEPALIVE'] = "type='default':0"
    assert hulot.exit_code == 2

@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_check_simple(monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': 'CHECK'}]
    hulot = Hulot()
    exit_code = hulot.run(False)
    assert exit_code == 0

@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_bad_command(monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': 'BAD_COMMAND', 'nodes': ['localhost0']}]
    hulot = Hulot()
    exit_code = hulot.run(False)
    assert exit_code == 1

@pytest.mark.usefixtures("minimal_db_initialization")
def test_hulot_check_nodes_to_remind(monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': 'CHECK'}]
    hulot = Hulot()
    hulot.nodes_list_to_remind = {'localhost0': {'timeout': -1, 'command': 'HALT'}}
    exit_code = hulot.run(False)
    print(hulot.nodes_list_running)
    assert 'localhost0' in hulot.nodes_list_running
    assert hulot.nodes_list_running['localhost0']['command'] == 'HALT'
    assert exit_code == 0
    
@pytest.mark.usefixtures("minimal_db_initialization")    
def test_hulot_check_wakeup_for_min_nodes(monkeypatch):
    # localhost2 to wakeup
    prev_value = config['ENERGY_SAVING_NODES_KEEPALIVE']
    config['ENERGY_SAVING_NODES_KEEPALIVE'] = "type='default':3"
    FakeZmq.recv_msgs[0] = [{'cmd': 'CHECK'}]
    hulot = Hulot()
    exit_code = hulot.run(False)
    config['ENERGY_SAVING_NODES_KEEPALIVE'] = prev_value
    print(hulot.nodes_list_running)
    assert 'localhost2' in hulot.nodes_list_running
    assert hulot.nodes_list_running['localhost2']['command'] == 'WAKEUP'
    assert exit_code == 0
    
@pytest.mark.usefixtures("minimal_db_initialization")        
def test_hulot_halt_1(monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': 'HALT', 'nodes': ['localhost0']}]
    hulot = Hulot()
    exit_code = hulot.run(False)
    print(hulot.nodes_list_running)    
    assert 'localhost0' in hulot.nodes_list_running
    assert hulot.nodes_list_running['localhost0']['command'] == 'HALT'
    assert exit_code == 0
    
@pytest.mark.usefixtures("minimal_db_initialization")        
def test_hulot_halt_keepalive(monkeypatch):
    prev_value = config['ENERGY_SAVING_NODES_KEEPALIVE']
    config['ENERGY_SAVING_NODES_KEEPALIVE'] = "type='default':3"
    FakeZmq.recv_msgs[0] = [{'cmd': 'HALT', 'nodes': ['localhost0']}]
    hulot = Hulot()
    #import pdb; pdb.set_trace()
    exit_code = hulot.run(False)
    config['ENERGY_SAVING_NODES_KEEPALIVE'] = prev_value
    print(hulot.nodes_list_running)
    assert 'localhost2' in hulot.nodes_list_running
    assert hulot.nodes_list_running['localhost2']['command'] == 'WAKEUP'
    assert exit_code == 0

@pytest.mark.usefixtures("minimal_db_initialization")     
def test_hulot_halt_1_forker(monkeypatch):
    config['ENERGY_SAVING_WINDOW_FORKER_BYPASS'] = 'no'
    FakeZmq.recv_msgs[0] = [{'cmd': 'HALT', 'nodes': ['localhost0']}]
    hulot = Hulot()
    exit_code = hulot.run(False)
    config['ENERGY_SAVING_WINDOW_FORKER_BYPASS'] = 'yes'
    print(hulot.nodes_list_running)    
    assert 'localhost0' in hulot.nodes_list_running
    assert hulot.nodes_list_running['localhost0']['command'] == 'HALT'
    assert exit_code == 0
    
@pytest.mark.usefixtures("minimal_db_initialization") 
def test_hulot_wakeup_1(monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': 'WAKEUP', 'nodes': ['localhost2']}]
    hulot = Hulot()
    exit_code = hulot.run(False)
    print(hulot.nodes_list_running)
    assert 'localhost2' in hulot.nodes_list_running
    assert hulot.nodes_list_running['localhost2']['command'] == 'WAKEUP'
    assert exit_code == 0
    
@pytest.mark.usefixtures("minimal_db_initialization") 
def test_hulot_wakeup_already_timeouted(monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': 'WAKEUP', 'nodes': ['localhost2']}]
    hulot = Hulot()
    hulot.nodes_list_running = {'localhost2': {'timeout': -1, 'command': 'WAKEUP'}}
    exit_code = hulot.run(False)
    print(hulot.nodes_list_running)
    assert hulot.nodes_list_running == {}
    assert exit_code == 0

@pytest.mark.usefixtures("minimal_db_initialization") 
def test_hulot_wakeup_already_pending(monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': 'WAKEUP', 'nodes': ['localhost2']}]
    hulot = Hulot()
    hulot.nodes_list_running = {'localhost2': {'timeout': tools.get_date()+1000,
                                               'command': 'WAKEUP'}}
    exit_code = hulot.run(False)
    print(hulot.nodes_list_running)
    print(hulot.nodes_list_to_remind)
    assert 'localhost2' in hulot.nodes_list_running
    assert hulot.nodes_list_to_remind == {}
    assert exit_code == 0

@pytest.mark.usefixtures("minimal_db_initialization") 
def test_hulot_halt_wakeup_already_pending(monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': 'HALT', 'nodes': ['localhost2']}]
    hulot = Hulot()
    hulot.nodes_list_running = {'localhost2': {'timeout': tools.get_date()+1000,
                                               'command': 'WAKEUP'}}
    exit_code = hulot.run(False)
    print(hulot.nodes_list_running)
    print(hulot.nodes_list_to_remind)
    assert 'localhost2' in hulot.nodes_list_running
    assert 'localhost2' in hulot.nodes_list_to_remind
    assert hulot.nodes_list_to_remind['localhost2']['command'] == 'HALT'
    assert exit_code == 0    
    
@pytest.mark.usefixtures("minimal_db_initialization") 
def test_hulot_check_clean_booted_node(monkeypatch):
    FakeZmq.recv_msgs[0] = [{'cmd': 'CHECK'}]
    hulot = Hulot()
    hulot.nodes_list_running = {'localhost0': {'timeout': -1, 'command': 'WAKEUP'}}
    exit_code = hulot.run(False)
    print(hulot.nodes_list_running)
    assert hulot.nodes_list_running == {}
    assert exit_code == 0

@pytest.mark.usefixtures("minimal_db_initialization") 
def test_hulot_wakeup_1_forker(monkeypatch):
    config['ENERGY_SAVING_WINDOW_FORKER_BYPASS'] = 'no'
    FakeZmq.recv_msgs[0] = [{'cmd': 'WAKEUP', 'nodes': ['localhost2']}]
    hulot = Hulot()
    exit_code = hulot.run(False)
    config['ENERGY_SAVING_WINDOW_FORKER_BYPASS'] = 'yes'
    print(hulot.nodes_list_running)
    assert 'localhost2' in hulot.nodes_list_running
    assert hulot.nodes_list_running['localhost2']['command'] == 'WAKEUP'
    assert exit_code == 0

def test_hulot_client(monkeypatch):
    hulot_ctl = HulotClient()
    hulot_ctl.check()
    assert FakeZmq.sent_msgs[0][0] == {'cmd': 'CHECK'}
    hulot_ctl.halt_nodes('localhost')
    assert FakeZmq.sent_msgs[0][1] == {'cmd': 'HALT', 'nodes': 'localhost'}
    hulot_ctl.wake_up_nodes('localhost')
    assert FakeZmq.sent_msgs[0][2] == {'cmd': 'WAKEUP', 'nodes': 'localhost'}
