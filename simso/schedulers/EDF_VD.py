from simso.schedulers import scheduler
from simso.core import Scheduler
from simso.core import Processor
from simso.core.Job import Job
from simso.core import Task
from simso.core import Timer
from enum import Enum

class ScheduleType(Enum):
    LOW_PRORITY = 0
    HIGH_PRIORITY = 1

class ScheduleState(Enum):
    INIT = 0
    ACTIVATE = 1
    TERMINATE = 2
    SCHEDULE = 3

EPSILON = 0.01
EPSILON_CY = 10000

@scheduler("simso.schedulers.EDF_VD")
class EDF_VD(Scheduler):
    schedule_type = ScheduleType.LOW_PRORITY
    # Hack : first schedule call is done after all Jobs are activated
    schedule_count = 0
    factor = 1.0
    timer = None

    def init(self):
        self.ready_list = []
        self.state = ScheduleState.INIT

    def on_activate(self, job: Job) -> None:
        #print(self.sim.now_ms(), " ", "OnActivate")
        self.ready_list.append(job)
        self.state = ScheduleState.ACTIVATE

        job.cpu.resched()

    def on_terminated(self, job: Job) -> None:
        self.state = ScheduleState.TERMINATE
        #print(self.sim.now_ms(), " ", "OnTerminate", job.task.identifier)
        self.ready_list.remove(job)

        # Abort all the LO jobs
        if self.schedule_type == ScheduleType.HIGH_PRIORITY and job.task.is_high_priority:
            low_prority_jobs = [x for x in self.ready_list if not x.task.is_high_priority]
            for job in low_prority_jobs:
                job.abort()

        job.cpu.resched()

    def on_mode_switch(self, cpu : Processor) -> None:
        print(self.sim.now_ms(), " ", "OnModeSwitch")
        if cpu.running.ret > EPSILON:
            self.schedule_type = ScheduleType.HIGH_PRIORITY
            self.sim.logger.log("Switched to HI priority")

    def virtual_deadline(self, job: Job) -> float:
        if job.task.is_high_priority:
            return job.activation_date + job.task.deadline * self.factor
        else:
            return job.activation_date + job.task.deadline

    def schedule(self, cpu: Processor) -> tuple[Job, Processor]:
        self.state = ScheduleState.SCHEDULE
        #print(self.sim.now_ms(), " ", "OnSchedule", self.schedule_type)

        self.schedule_count += 1

        if self.schedule_count == 1:
            self.factor = self._check_schedulability()

        if self.timer is not None:
            self.timer.stop()

        if self.schedule_type == ScheduleType.LOW_PRORITY:
            if self.ready_list:
                # job with the lowest virtual deadline
                # ignore aborted
                not_aborted = [x for x in self.ready_list if not x.aborted]
                job = min(not_aborted, key=lambda x: self.virtual_deadline(x))
            else:
                job = None
        elif self.schedule_type == ScheduleType.HIGH_PRIORITY:
            high_prority_jobs = [x for x in self.ready_list if x.task.is_high_priority]
            if high_prority_jobs:                
                job = min(high_prority_jobs, key=lambda x: x.absolute_deadline)
            else:
                # Switch back to the LO if no more HI tasks
                if len(self.ready_list) - len(high_prority_jobs):
                    self.schedule_type = ScheduleType.LOW_PRORITY
                    self.sim.logger.log("Switched to LO priority")
                    not_aborted = [x for x in self.ready_list if not x.aborted]
                    job = min(not_aborted, key=lambda x: self.virtual_deadline(x))
                else:
                    job = None

        if self.schedule_type == ScheduleType.LOW_PRORITY and job is not None:
            # Init timer to know if Job is executed for more than C(LO)
            self.timer = Timer(
                self.sim, EDF_VD.on_mode_switch, (self, self.processors[0]), job.task.wcet_lo + EPSILON,
                cpu=self.processors[0], in_ms=True, one_shot=True)

            self.timer.start()

        return (job, cpu)
    
    def _check_schedulability(self) -> float:
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
            self.sim.logger.log(f"System is not schedulable for given x={self.factor}")
            return 1

