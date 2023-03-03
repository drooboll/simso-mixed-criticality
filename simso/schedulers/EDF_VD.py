from simso.schedulers import scheduler
from simso.core import Scheduler
from simso.core import Processor
from simso.core import Job
from simso.core import Task
from enum import Enum

class ScheduleType(Enum):
    LOW_PRORITY = 0
    HIGH_PRIORITY = 1

class ScheduleState(Enum):
    INIT = 0
    ACTIVATE = 1
    TERMINATE = 2
    SCHEDULE = 3

@scheduler("simso.schedulers.EDF_VD")
class EDF_VD(Scheduler):
    schedule_type = ScheduleType.LOW_PRORITY
    # Hack : first schedule call is done after all Jobs are activated
    schedule_count = 0
    factor = 1.0

    def init(self):
        self.ready_list = []
        self.state = ScheduleState.INIT

    def on_activate(self, job: Job):
        print(self.sim.now_ms(), " ", "OnActivate")
        self.ready_list.append(job)
        self.state = ScheduleState.ACTIVATE

        job.cpu.resched()

    def on_terminated(self, job: Job):
        self.state = ScheduleState.TERMINATE
        print(self.sim.now_ms(), " ", "OnTerminate")
        self.ready_list.remove(job)

        job.cpu.resched()

    def schedule(self, cpu: Processor):
        self.schedule_count += 1

        if self.schedule_count == 1:
            self.factor = self._check_schedulability()

        self.state = ScheduleState.SCHEDULE
        print(self.sim.now_ms(), " ", "OnSchedule")
        if self.ready_list:
            # job with the lowest virtual deadline
            # ignore aborted
            job = min([x for x in self.ready_list if not x.aborted],
                      key=lambda x: x.absolute_deadline if not x.task.is_high_priority else x.absolute_deadline * self.factor)
        else:
            job = None

        return (job, cpu)
    
    def _check_schedulability(self):
        utilisation_hi_lo = 0
        utilisation_lo_lo = 0
        utilisation_hi_hi = 0

        for job in self.ready_list:
            if job.task.is_high_priority:
                utilisation_hi_hi += job.task.wcet_hi / job.period
                utilisation_hi_lo += job.task.wcet_lo / job.period
            else:
                utilisation_lo_lo += job.task.wcet_lo / job.period
        
        print(f"LO LO: {utilisation_lo_lo}, LO HI: {utilisation_hi_lo}, HI HI: {utilisation_hi_hi}")
        left = utilisation_hi_lo / (1 - utilisation_lo_lo)
        right = (1 - utilisation_hi_hi) / utilisation_lo_lo
        print(f"Left: {left}, right: {right}")

        if left <= right:
            return (left + right) * 0.5
        
        else:
            print("Not schedulable :(")
            return 1

