import unittest
from kao.job import *
from kao.scheduling import *

class TestScheduling(unittest.TestCase):

    def compare_slots_val_ref(self, slots, v):
        sid = 1
        i = 0
        while True:
            slot = slots[sid]
            (b,e,itvs) = v[i]
            if (slot.b != b) or (slot.e != e) or not equal_itvs(slot.itvs, itvs):
                return False
            sid = slot.next
            if (sid == 0):
                break
            i += 1
        return True

    def test_set_slots_with_prev_scheduled_jobs_1(self):
        v = [ ( 1 , 4 , [(1, 32)] ),
              ( 5 , 14 , [(1, 9), (21, 32)] ),
              ( 15 , 29 , [(1, 32)] ),
              ( 30 , 49 , [(1, 4), (16, 19), (29, 32)] ),
              ( 50 , 100 , [(1, 32)] )
              ]

        j1 = Job(1,"", 5, 10, "", "", "", {}, [(10, 20)], 1, [])
        j2 = Job(1,"", 30, 20, "", "", "", {}, [(5, 15),(20, 28)], 1, [])
        ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 1, 100))
        all_ss = {0:ss}

        set_slots_with_prev_scheduled_jobs(all_ss, {1:j1, 2:j2}, [1,2], 10)

        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))

#    def 

    def test_schedule_id_jobs_ct(self):
        
#        schedule_id_jobs_ct(slots_set, jobs, hy, req_jobs_status???, id_jobs, security_time):
        sched = []
        self.assertEqual(sched,[]) 

if __name__ == '__main__':
    unittest.main()
