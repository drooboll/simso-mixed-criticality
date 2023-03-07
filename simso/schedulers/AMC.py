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
        self.priorities_lo = {}
        self.priorities_lo_hi = {}
        self.priorities_hi = {}
        self.state = ScheduleState.INIT
        self.schedule_type = ScheduleType.LOW_PRORITY
        self.previous_schedule_type = ScheduleType.LOW_PRORITY

    def on_activate(self, job: Job) -> None:
        self.ready_list.append(job)
        self.state = ScheduleState.ACTIVATE

        job.cpu.resched()

    def on_terminated(self, job: Job) -> None:
        self.state = ScheduleState.TERMINATE
        self.ready_list.remove(job)

        job.cpu.resched()

    def on_wcet_passed(self, cpu : Processor) -> None:
        # Abort low-priority task if executed for too long
        if not cpu.running.task.is_high_priority:
            cpu.running.abort()
            cpu.resched()
            return
        
        if cpu.running.ret > EPSILON:
            self._update_type(ScheduleType.HIGH_PRIORITY)

            # Abort all pending LO tasks
            for job in self.ready_list:
                if not job.task.is_high_priority:
                    job.abort()

    def _update_type(self, new_type: ScheduleType) -> None:
        self.sim.logger.log(f"Switching mode to {new_type}")
        self.previous_schedule_type = self.schedule_type
        self.schedule_type = new_type

    def get_more_prioritized_jobs(self, job: Job) -> list[Job]:
        job_prio = self.priorities_lo[job.task.identifier]

        other_jobs = [x for x in self.ready_list if x != job]
        return [x for x in other_jobs if self.priorities_lo[x.task.identifier] >= job_prio]
    
    def get_more_prioritized_jobs_hi(self, job: Job) -> list[Job]:
        if not job.task.is_high_priority:
            return [x for x in self.ready_list if x.task.is_high_priority]
        
        job_prio = self.priorities_hi[job.task.identifier]

        other_jobs = [x for x in self.ready_list if x != job and x.task.is_high_priority]
        return [x for x in other_jobs if self.priorities_lo[x.task.identifier] >= job_prio]
    
    def get_more_prioritized_jobs_lo(self, job: Job) -> list[Job]:
        if job.task.is_high_priority:
            return []
        
        job_prio = self.priorities_lo[job.task.identifier]

        other_jobs = [x for x in self.ready_list if x != job and not x.task.is_high_priority]
        return [x for x in other_jobs if self.priorities_lo[x.task.identifier] >= job_prio]

    def find_response_time_lo(self, job: Job) -> float:
        resp = job.task.wcet_lo
        new_resp = 0

        higher_priority = self.get_more_prioritized_jobs(job) 
        while new_resp != resp and resp < job.deadline:
            new_resp = resp
            resp = job.task.wcet_lo + sum([x.task.wcet_lo * math.ceil(resp / x.period) for x in higher_priority])

        return resp
    
    def find_response_time_hi(self, job: Job) -> float:
        resp = job.task.wcet_hi
        new_resp = 0

        higher_priority = self.get_more_prioritized_jobs_hi(job)
        while new_resp != resp and resp < job.deadline:
            new_resp = resp
            resp = job.task.wcet_hi + sum([x.task.wcet_hi * math.ceil(resp / x.period) for x in higher_priority])

        return resp
    
    def find_response_time_lo_hi(self, job: Job) -> float:
        resp = job.task.wcet_hi
        new_resp = 0

        higher_priority_lo = self.get_more_prioritized_jobs_lo(job)
        higher_priority_hi = self.get_more_prioritized_jobs_hi(job)
        while new_resp != resp and resp < job.deadline:
            new_resp = resp
            resp = job.task.wcet_hi + sum([x.task.wcet_lo * math.ceil(resp / x.period) for x in higher_priority_lo]) +\
                sum([x.task.wcet_hi * math.ceil(resp / x.period) for x in higher_priority_hi])

        return resp
    
    def _schedule_lo(self, cpu: Processor) -> Job:
        not_aborted = [x for x in self.ready_list if not x.aborted]

        if not_aborted:
            # job with the highest priority
            return max(not_aborted, key=lambda x: self.priorities_lo[x.task.identifier])

        return None
            
    def _try_schedule_lo_hi(self) -> Job:
        high_prority_jobs = [x for x in self.ready_list if x.task.is_high_priority]

        if high_prority_jobs:
            return max(high_prority_jobs, key=lambda x: self.priorities_lo_hi[x.task.identifier])
    
        return None
    
    def _try_schedule_hi(self) -> Job:
        high_prority_jobs = [x for x in self.ready_list if x.task.is_high_priority]

        if high_prority_jobs:
            return max(high_prority_jobs, key=lambda x: self.priorities_hi[x.task.identifier])
        
        return None

    def schedule(self, cpu: Processor):
        self.state = ScheduleState.SCHEDULE
        self.schedule_count += 1

        if self.schedule_count == 1:
            self._find_schedule_order()
            self._find_schedule_order_hi()
            self._find_schedule_order_lo_hi()
            self._log_scheduling_order()

        if self.timer is not None:
            self.timer.stop()
        
        if self.schedule_type == ScheduleType.LOW_PRORITY:
            job = self._schedule_lo(cpu)
        elif self.schedule_type == ScheduleType.HIGH_PRIORITY:
            if self.previous_schedule_type == ScheduleType.LOW_PRORITY:
                job = self._try_schedule_lo_hi()
            else:
                job = self._try_schedule_hi()

            if job is None:
                self._update_type(ScheduleType.LOW_PRORITY)
                job = self._schedule_lo(cpu)

        if self.schedule_type == ScheduleType.LOW_PRORITY and job is not None:
            # Init timer to know if Job is executed for more than C(LO)
            self.timer = Timer(
                self.sim,
                AMC.on_wcet_passed,
                (self, self.processors[0]),
                job.task.wcet_lo + EPSILON,
                cpu=self.processors[0],
                in_ms=True, one_shot=True
            )

            self.timer.start()

        return (job, cpu)
    
    def _log_scheduling_order(self) -> None:
        # Assume that in the ready_list there are jobs from all tasks
        priorities_lo = ', '.join([f"{x.task.name} : {self.priorities_lo[x.task.identifier]}" for x in self.ready_list])
        priorities_lo_hi = ', '.join(
            [
                f"{x.task.name} : {self.priorities_lo_hi[x.task.identifier]}"
                for x in self.ready_list if x.task.is_high_priority
            ]
        )
        priorities_hi = ', '.join(
            [
                f"{x.task.name} : {self.priorities_hi[x.task.identifier]}"
                for x in self.ready_list if x.task.is_high_priority
            ]
        )

        self.sim.logger.log(f"Priorities in LO: [{priorities_lo}]")
        self.sim.logger.log(f"Priorities in LO->HI: [{priorities_lo_hi}]")
        self.sim.logger.log(f"Priorities in HI: [{priorities_hi}]")
    
    # Down here is a copy-paste code, sorry :(
    def _find_schedule_order(self) -> None:
        unordered = self.ready_list.copy()
        unordered = sorted(unordered, key=lambda x: x.task.is_high_priority)
        ordered = []

        for job in unordered:
            self.priorities_lo[job.task.identifier] = 1

        while len(ordered) != self.ready_list:
            ordered_len = len(ordered)
            if len(unordered) == 1:
                ordered.append(unordered[0])
                break
            for job in unordered:
                self.priorities_lo[job.task.identifier] += 1
            for i in range(len(unordered)):
                self.priorities_lo[unordered[i].task.identifier] -= 1
                resp = self.find_response_time_lo(unordered[i])
                if resp <= unordered[i].deadline:
                    ordered.append(unordered[i])
                    unordered.remove(unordered[i])
                    break
            
            # No job is added -> scheduling not possible
            if len(ordered) == ordered_len:
                self.sim.logger.log("Error in priority assignment in LO mode!")
                break

    def _find_schedule_order_hi(self) -> None:
        unordered = [x for x in self.ready_list if x.task.is_high_priority]
        ordered = []

        for job in unordered:
            self.priorities_hi[job.task.identifier] = 1

        ordered_len = len(ordered)
        while len(ordered) != self.ready_list:
            ordered_len = len(ordered)
            if len(unordered) == 1:
                ordered.append(unordered[0])
                break
            for job in unordered:
                self.priorities_hi[job.task.identifier] += 1
            for i in range(len(unordered)):
                self.priorities_hi[unordered[i].task.identifier] -= 1
                resp = self.find_response_time_hi(unordered[i])
                if resp <= unordered[i].deadline:
                    ordered.append(unordered[i])
                    unordered.remove(unordered[i])
                    break
            
            # No job is added -> scheduling not possible
            if len(ordered) == ordered_len:
                self.sim.logger.log("Error in priority assignment in LO->HI mode!")
                break
    
    def _find_schedule_order_lo_hi(self) -> None:
        unordered = [x for x in self.ready_list if x.task.is_high_priority]
        ordered = []

        for job in unordered:
            self.priorities_lo_hi[job.task.identifier] = 1

        while len(ordered) != self.ready_list:
            ordered_len = len(ordered)
            if len(unordered) == 1:
                ordered.append(unordered[0])
                break
            for job in unordered:
                self.priorities_lo_hi[job.task.identifier] += 1
            for i in range(len(unordered)):
                self.priorities_lo_hi[unordered[i].task.identifier] -= 1
                resp = self.find_response_time_lo_hi(unordered[i])
                if resp <= unordered[i].deadline:
                    ordered.append(unordered[i])
                    unordered.remove(unordered[i])
                    break
            
            # No job is added -> scheduling not possible
            if len(ordered) == ordered_len:
                self.sim.logger.log("Error in priority assignment in HI mode!")
                break
