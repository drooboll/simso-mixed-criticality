from simso.schedulers import scheduler
from simso.core import Scheduler
from simso.core import Processor
from simso.core import Job
from simso.core import Task
from simso.core import Timer
from enum import Enum

import math

class ScheduleType(Enum):
    LOW_PRORITY = 0
    HIGH_PRIORITY = 1

class ScheduleState(Enum):
    INIT = 0
    ACTIVATE = 1
    TERMINATE = 2
    SCHEDULE = 3

EPSILON = 0.1
EPSILON_CY = 10000

@scheduler("simso.schedulers.AMC")
class AMC(Scheduler):
    schedule_type = ScheduleType.LOW_PRORITY
    # Hack : first schedule call is done after all Jobs are activated
    schedule_count = 0
    factor = 1.0
    timer = None

    def init(self):
        self.ready_list = []
        self.priorities = {}
        self.priorities_lo_hi = {}
        self.priorities_hi = {}
        self.state = ScheduleState.INIT
        self.schedule_type = ScheduleType.LOW_PRORITY
        self.previous_schedule_type = ScheduleType.LOW_PRORITY

    def on_activate(self, job: Job):
        print(self.sim.now_ms(), " ", "OnActivate")
        self.ready_list.append(job)
        self.state = ScheduleState.ACTIVATE

        job.cpu.resched()

    def on_terminated(self, job: Job):
        self.state = ScheduleState.TERMINATE
        print(self.sim.now_ms(), " ", "OnTerminate", job.task.identifier)
        self.ready_list.remove(job)

        # Abort all the LO jobs
        if self.schedule_type == ScheduleType.HIGH_PRIORITY and job.task.is_high_priority:
            low_prority_jobs = [x for x in self.ready_list if not x.task.is_high_priority]
            for job in low_prority_jobs:
                job.abort()

        job.cpu.resched()

    def on_mode_switch(self, cpu : Processor):
        print(self.sim.now_ms(), " ", "OnModeSwitch")
        # Abort low-priority task if executed for too long
        if not cpu.running.task.is_high_priority:
            cpu.running.abort()
            cpu.resched()
            return
        
        if cpu.running.ret > EPSILON:
            self.schedule_type = ScheduleType.HIGH_PRIORITY
            self.previous_schedule_type = ScheduleType.LOW_PRORITY


    def get_more_prioritized_jobs(self, job: Job):
        job_prio = self.priorities[job.task.identifier]

        other_jobs = [x for x in self.ready_list if x != job]
        return [x for x in other_jobs if self.priorities[x.task.identifier] >= job_prio]
    
    def get_more_prioritized_jobs_hi(self, job: Job):
        if not job.task.is_high_priority:
            return [x for x in self.ready_list if x.task.is_high_priority]
        
        job_prio = self.priorities_hi[job.task.identifier]

        other_jobs = [x for x in self.ready_list if x != job and x.task.is_high_priority]
        return [x for x in other_jobs if self.priorities[x.task.identifier] >= job_prio]
    
    def get_more_prioritized_jobs_lo(self, job: Job):
        if job.task.is_high_priority:
            return []
        
        job_prio = self.priorities_lo[job.task.identifier]

        other_jobs = [x for x in self.ready_list if x != job and not x.task.is_high_priority]
        return [x for x in other_jobs if self.priorities[x.task.identifier] >= job_prio]

    def find_response_time_lo(self, job: Job):
        resp = job.task.wcet_lo
        new_resp = 0

        higher_priority = self.get_more_prioritized_jobs(job) 
        while new_resp != resp or resp > job.deadline:
            new_resp = resp
            resp = job.task.wcet_lo + sum([x.task.wcet_lo * math.ceil(resp / x.period) for x in higher_priority])

        print(f"LO:Resp for {job.task.identifier} is {resp}")
        return resp
    
    def find_response_time_hi(self, job: Job):
        resp = job.task.wcet_hi
        new_resp = 0

        higher_priority = self.get_more_prioritized_jobs_hi(job)
        while new_resp != resp or resp > job.deadline:
            new_resp = resp
            resp = job.task.wcet_hi + sum([x.task.wcet_hi * math.ceil(resp / x.period) for x in higher_priority])

        print(f"HI:Resp for {job.task.identifier} is {resp}")
        return resp
    
    def find_response_time_lo_hi(self, job: Job):
        resp = job.task.wcet_hi
        new_resp = 0

        higher_priority_lo = self.get_more_prioritized_jobs_lo(job)
        higher_priority_hi = self.get_more_prioritized_jobs_hi(job)
        while new_resp != resp or resp > job.deadline:
            new_resp = resp
            resp = job.task.wcet_hi + sum([x.task.wcet_lo * math.ceil(resp / x.period) for x in higher_priority_lo]) +\
                sum([x.task.wcet_hi * math.ceil(resp / x.period) for x in higher_priority_hi])

        print(f"LO_HI:Resp for {job.task.identifier} is {resp}")
        return resp

    def schedule(self, cpu: Processor):
        self.state = ScheduleState.SCHEDULE
        print(self.sim.now_ms(), " ", "OnSchedule", self.schedule_type)

        self.schedule_count += 1

        if self.schedule_count == 1:
            self._find_schedule_order()
            print(self.priorities)
            self._find_schedule_order_hi()
            print(self.priorities_hi)
            self._find_schedule_order_lo_hi()
            print(self.priorities_lo_hi)

        if self.timer is not None:
            self.timer.stop()

        not_aborted = [x for x in self.ready_list if not x.aborted]
        if self.schedule_type == ScheduleType.LOW_PRORITY:
            if self.ready_list:
                # job with the highest priority
                job = max(not_aborted, key=lambda x: self.priorities[x.task.identifier])
            else:
                job = None
        elif self.schedule_type == ScheduleType.HIGH_PRIORITY:
            high_prority_jobs = [x for x in self.ready_list if x.task.is_high_priority]
            if high_prority_jobs:
                if self.previous_schedule_type == ScheduleType.LOW_PRORITY:
                    job = max(not_aborted, key=lambda x: self.priorities_lo_hi[x.task.identifier])
                else:
                    job = max(not_aborted, key=lambda x: self.priorities_hi[x.task.identifier])

                high_prority_jobs = [x for x in self.ready_list if x.task.is_high_priority]
            else:
                # Switch back to the LO if no more HI tasks
                if len(self.ready_list) - len(high_prority_jobs):
                    self.schedule_type = ScheduleType.LOW_PRORITY
                    job = max(not_aborted, key=lambda x: self.priorities[x.task.identifier])
                else:
                    job = None

        if self.schedule_type == ScheduleType.LOW_PRORITY and job is not None:
            # Init timer to know if Job is executed for more than C(LO)
            self.timer = Timer(
                self.sim, AMC.on_mode_switch, (self, self.processors[0]), job.task.wcet_lo + EPSILON,
                cpu=self.processors[0], in_ms=True, one_shot=True)

            self.timer.start()

        return (job, cpu)
    
    def _find_schedule_order(self):
        # First find order in LO
        unordered = self.ready_list.copy()
        ordered = []

        for job in unordered:
            self.priorities[job.task.identifier] = 1

        while len(ordered) != self.ready_list:
            if len(unordered) == 1:
                ordered.append(unordered[0])
                break
            for job in unordered:
                self.priorities[job.task.identifier] += 1
            for i in range(len(unordered)):
                self.priorities[unordered[i].task.identifier] -= 1
                print("Avail prio:", self.priorities)
                resp = self.find_response_time_lo(unordered[i])
                if resp < unordered[i].deadline:
                    ordered.append(unordered[i])
                    unordered.remove(unordered[i])
                    break

        self.priorities = {}
        for i in range(len(ordered)):
            self.priorities[ordered[i].task.identifier] = i

    def _find_schedule_order_hi(self):
        unordered = self.ready_list.copy()
        ordered = []

        for job in unordered:
            self.priorities_hi[job.task.identifier] = 1

        while len(ordered) != self.ready_list:
            if len(unordered) == 1:
                ordered.append(unordered[0])
                break
            for job in unordered:
                self.priorities_hi[job.task.identifier] += 1
            for i in range(len(unordered)):
                self.priorities_hi[unordered[i].task.identifier] -= 1
                resp = self.find_response_time_hi(unordered[i])
                if resp < unordered[i].deadline:
                    ordered.append(unordered[i])
                    unordered.remove(unordered[i])
                    break

        self.priorities_hi = {}
        for i in range(len(ordered)):
            self.priorities_hi[ordered[i].task.identifier] = i
    
    def _find_schedule_order_lo_hi(self):
        unordered = self.ready_list.copy()
        ordered = []

        for job in unordered:
            self.priorities_lo_hi[job.task.identifier] = 1

        while len(ordered) != self.ready_list:
            if len(unordered) == 1:
                ordered.append(unordered[0])
                break
            for job in unordered:
                self.priorities_lo_hi[job.task.identifier] += 1
            for i in range(len(unordered)):
                self.priorities_lo_hi[unordered[i].task.identifier] -= 1
                resp = self.find_response_time_lo_hi(unordered[i])
                if resp < unordered[i].deadline:
                    ordered.append(unordered[i])
                    unordered.remove(unordered[i])
                    break

        self.priorities_lo_hi = {}
        for i in range(len(ordered)):
            self.priorities_lo_hi[ordered[i].task.identifier] = i
