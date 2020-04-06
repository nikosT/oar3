# coding: utf-8
import pytest

from procset import ProcSet

from codecs import open
from copy import deepcopy
from tempfile import mkstemp

import time
from datetime import (date, datetime, timedelta)

#from oar.lib.job_handling import JobPseudo
from oar.kao.slot import Slot, SlotSet
#from oar.kao.scheduling import (schedule_id_jobs_ct,
#                                set_slots_with_prev_scheduled_jobs)

from oar.kao.quotas import Quotas, Calendar
import oar.lib.resource as rs

from oar.lib import config, get_logger

# import pdb

config['LOG_FILE'] = ':stderr:'
logger = get_logger("oar.test")

"""
    quotas[queue, project, job_type, user] = [int, int, float];
                                               |    |     |
              maximum used resources ----------+    |     |
              maximum number of running jobs -------+     |
              maximum resources times (hours) ------------+
"""

json_example_full = {
    "periodical": [
        ["08:00-19:00 mon-fri * *", "quotas_workday", "workdays"],
        ["19:00-00:00 mon-thu * *", "quotas_nigth", "nights of workdays"],
        ["00:00-08:00 tue-fri * *", "quotas_nigth", "nights of workdays"],
        ["19:00-00:00 fri * *", "quotas_weekend", "weekend"],
        ["* sat-sun * *", "quotas_weekend", "weekend"],
        ["00:00-08:00 mon * *", "quotas_weekend", "weekend"]
    ],

    "oneshot": [
        ["2020-07-23 19:30", "2020-08-29 8:30", "quotas_holiday", "summer holiday"],
        ["2020-03-16 19:30", "2020-05-10 8:30", "quotas_holiday", "confinement"]
    ],
    "quotas_workday": {
        "*,*,*,john": [100,-1,-1],
        "*,projA,*,*": [200,-1,-1]
    },
    "quotas_nigth": {
        "*,*,*,john": [100,-1,-1],
        "*,projA,*,*": [200,-1,-1]
    },
    "quotas_weekend": {
        "*,*,*,john": [100,-1,-1],
        "*,projA,*,*": [200,-1,-1]
    },
    "quotas_holiday": {
        "*,*,*,john": [100,-1,-1],
        "*,projA,*,*": [200,-1,-1]
    }
}

json_example_simple = {
    "periodical": [
        ["* mon-wed * *", "quotas_1", "test1"],
        ["* thu-sun * *", "quotas_2", "test2"]
    ],
    "quotas_1": {
        "*,*,*,john": [10,-1,-1],
        "*,projA,*,*": [20,-1,-1]
    },
    "quotas_2": {
        "*,*,*,lili": [20,-1,-1],
        "*,projB,*,*": [15,-1,-1]
    }
}

def compare_slots_val_ref(slots, v):
    sid = 1
    i = 0
    while True:
        slot = slots[sid]
        (b, e, itvs) = v[i]
        if ((slot.b != b) or (slot.e != e)
                or not slot.itvs == itvs):
            return False
        sid = slot.next
        if (sid == 0):
            break
        i += 1
    return True


@pytest.fixture(scope='module', autouse=True)
def oar_conf(request):
    config['QUOTAS'] = 'yes'

    def remove_quotas():
        config['QUOTAS'] = 'no'

    request.addfinalizer(remove_quotas)


@pytest.fixture(scope='function', autouse=True)
def reset_quotas():
    Quotas.enabled = False
    Quotas.temporal = False
    Quotas.rules = {}
    Quotas.job_types = ['*']

def period_weekstart():
    t_dt = datetime.fromtimestamp(time.time()).date()
    t_weekstart_day_dt = t_dt - timedelta(days=t_dt.weekday())
    return int(datetime.combine(t_weekstart_day_dt, datetime.min.time()).timestamp())

def test_calendar_periodical_fromJson():

    calendar = Calendar(json_example_full)
    print()
    calendar.show()
    
    check, periodical_id = calendar.check_periodicals()

    print(check, periodical_id)
    #import pdb; pdb.set_trace()
    assert check

def test_calendar_periodical_fromJson_bad():
    assert True
    #    pass
    # ["09:00-19:00 mon-fri * *", "quotas_workday", "workdays"],


def test_calendar_rules_at():
    config["QUOTAS_PERIOD"] =  3*7*86400 # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(json_example_simple)
    Quotas.calendar.show()
    t0 = period_weekstart()

    quotas_rules_id, remaining_period = Quotas.calendar.rules_at(t0)

    assert quotas_rules_id == 0
    assert remaining_period == 259200
    
def test_calendar_simple_slotSet_1():
    config["QUOTAS_PERIOD"] =  3*7*86400 # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(json_example_simple)
    res = ProcSet(*[(1, 32)])
    t0 = period_weekstart()
    ss = SlotSet(Slot(1, 0, 0, res, t0, t0 + 3*86400))
    print(ss)
    assert ss.slots[1].quotas_rules_id == 0
    
def test_calendar_simple_slotSet_2():
    config["QUOTAS_PERIOD"] =  3*7*86400 # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(json_example_simple)
    check, periodical_id = Quotas.calendar.check_periodicals()
    res = ProcSet(*[(1, 32)])
    t0 = period_weekstart()
    ss = SlotSet(Slot(1, 0, 0, res, t0, t0 + 4*86400))
    
    print(ss)
    assert ss.slots[1].quotas_rules_id == 0
    assert ss.slots[2].quotas_rules_id == 1
    assert ss.slots[1].e-ss.slots[1].b == 3 * 86400 -1
    
def test_calendar_simple_slotSet_3():
    config["QUOTAS_PERIOD"] =  3*7*86400 # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(json_example_simple)
    check, periodical_id = Quotas.calendar.check_periodicals()
    res = ProcSet(*[(1, 32)])
    t0 = period_weekstart()
    ss = SlotSet(Slot(1, 0, 0, res, t0, t0 + 5*86400))
    assert ss.slots[1].quotas_rules_id == 0
    assert ss.slots[2].quotas_rules_id == 1
    assert ss.slots[1].e-ss.slots[1].b == 3 * 86400 -1

def test_calendar_simple_slotSet_4():
    config["QUOTAS_PERIOD"] =  3*7*86400 # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(json_example_simple)
    Quotas.calendar.show()
    check, periodical_id = Quotas.calendar.check_periodicals()
    print(check, periodical_id)
    res = ProcSet(*[(1, 32)])
    t0 = period_weekstart()
    t1 = t0 + 2*7*86400 - 1

    ss = SlotSet(Slot(1, 0, 0, res, t0, t1))

    print('t0: {} t1: {} t1-t0'.format(t0, t1, t1-t0))
    print(ss)
    v = []
    i = 1
    while i:
        s = ss.slots[i]
        print('Slot: {}  duration: {} quotas_rules_id: {}'.format(i, s.e-s.b+1, s.quotas_rules_id))
        v.append((i, s.e-s.b+1, s.quotas_rules_id))
        i = s.next
    
    assert ss.slots[1].quotas_rules_id == 0
    assert ss.slots[2].quotas_rules_id == 1
    assert ss.slots[1].e-ss.slots[1].b == 3 * 86400 -1
    assert v == [(1, 259200, 0), (2, 345600, 1), (3, 259200, 0), (4, 345600, 1)]
    
def test_calendar_simple_slotSet_5():
    config["QUOTAS_PERIOD"] =  3*7*86400 # 3 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(json_example_simple)
    Quotas.calendar.show()
    check, periodical_id = Quotas.calendar.check_periodicals()
    print(check, periodical_id)
    res = ProcSet(*[(1, 32)])
    t0 = period_weekstart()
    t1 = t0 + 2*7*86400

    ss = SlotSet(Slot(1, 0, 0, res, t0, t1))

    print('t0: {} t1: {} t1-t0'.format(t0, t1, t1-t0))
    print(ss)
    v = []
    i = 1
    while i:
        s = ss.slots[i]
        print('Slot: {}  duration: {} quotas_rules_id: {}'.format(i, s.e-s.b+1, s.quotas_rules_id))
        v.append((i, s.e-s.b+1, s.quotas_rules_id))
        i = s.next
    
    assert ss.slots[1].quotas_rules_id == 0
    assert ss.slots[2].quotas_rules_id == 1
    assert ss.slots[1].e-ss.slots[1].b == 3 * 86400 -1
    assert v == [(1, 259200, 0), (2, 345600, 1), (3, 259200, 0), (4, 345600, 1), (5, 1, 0)]
    
def test_calendar_simple_slotSet_5():
    config["QUOTAS_PERIOD"] =  7*86400 # 1 weeks
    Quotas.enabled = True
    Quotas.calendar = Calendar(json_example_simple)
    Quotas.calendar.show()
    check, periodical_id = Quotas.calendar.check_periodicals()
    print(check, periodical_id)
    res = ProcSet(*[(1, 32)])
    t0 = period_weekstart()
    t1 = t0 + 2*7*86400

    ss = SlotSet(Slot(1, 0, 0, res, t0, t1))

    print('t0: {} t1: {} t1-t0'.format(t0, t1, t1-t0))
    print(ss)
    v = []
    i = 1
    while i:
        s = ss.slots[i]
        print('Slot: {}  duration: {} quotas_rules_id: {}'.format(i, s.e-s.b+1, s.quotas_rules_id))
        v.append((i, s.e-s.b+1, s.quotas_rules_id))
        i = s.next
    
    assert ss.slots[1].quotas_rules_id == 0
    assert ss.slots[2].quotas_rules_id == 1
    assert ss.slots[1].e-ss.slots[1].b == 3 * 86400 -1
    assert v == [(1, 259200, 0), (2, 345600, 1), (3, 604801, -1)]

def test_calendar_simple_slotSet_multi_slot_1():
    assert True
