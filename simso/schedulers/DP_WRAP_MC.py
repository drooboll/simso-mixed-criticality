"""
Implementation of the DP-WRAP algorithm as presented by Levin et al. in
"DP-FAIR: A Simple Model for Understanding Optimal Multiprocessor Scheduling".
"""
from simso.core import Scheduler, Timer
from simso.core.Job import Job
from math import ceil
from simso.schedulers import scheduler
from enum import Enum

class ScheduleType(Enum):
    LOW_PRORITY = 0
    HIGH_PRIORITY = 1

@scheduler("simso.schedulers.DP_WRAP_MC")
class DP_WRAP_MC(Scheduler):
    def init(self):
        self.t_f = 0
        self.waiting_schedule = False
        self.mirroring = False
        self.allocations = []
        self.timers = {}
        self.schedule_type = ScheduleType.LOW_PRORITY
        self.previous_schedule_type = ScheduleType.LOW_PRORITY

    def _update_type(self, new_type: ScheduleType) -> None:
        if new_type != self.schedule_type:
            self.sim.logger.log(f"Switching mode to {new_type}")
        self.previous_schedule_type = self.schedule_type
        self.schedule_type = new_type

    def reschedule(self, cpu=None):
        """
        Ask for a scheduling decision. Don't call if not necessary.
        """
        if not self.waiting_schedule:
            if cpu is None:
                cpu = self.processors[0]
            cpu.resched()
            self.waiting_schedule = True

    def on_activate(self, job):
        self.reschedule()

    def _get_compute_time_ms(self, job: Job) -> float:
        if self.schedule_type == ScheduleType.LOW_PRORITY:
            return job.task.wcet
        return job.ret
    
    def init_interval(self):
        """
        Determine the end of the interval and compute allocation of the jobs
        to the processors using the McNaughton algorithm.
        """
        self.allocations = [[0, []] for _ in self.processors]

        task_list = self.task_list if self.schedule_type == ScheduleType.LOW_PRORITY else [x for x in self.task_list if x.is_high_priority]
        task_list = sorted(task_list, key= lambda x: x.is_high_priority, reverse=True)
        self.t_f = ceil(min([x.job.absolute_deadline for x in task_list]
                            ) * self.sim.cycles_per_ms)
        # Duration that can be allocated for each processor.
        w = int(self.t_f - self.sim.now())
        proc_id = 0  # Processor id.
        print(f"{self.sim.now_ms()}: Start ordering, period {w}")

        hi_tasks = [x for x in task_list if x.is_high_priority]
        lo_tasks = [x for x in task_list if not x.is_high_priority]

        if self.schedule_type == ScheduleType.HIGH_PRIORITY and self.previous_schedule_type == ScheduleType.LOW_PRORITY:
            for task in hi_tasks:
                job = task.job
                if not job.is_active():
                    continue
                # What's left to compute
                duration = ceil(w * ((task.wcet_hi - job.actual_computation_time) / (job.absolute_deadline - self.sim.now_ms())))
                if self.allocations[proc_id][0] + duration <= w:
                    self.allocations[proc_id][1].append((job, duration))
                    # Stub for worst-case
                    self.allocations[proc_id][0] += duration
                else:
                    duration1 = w - self.allocations[proc_id][0]

                    if duration1 > 0:
                        self.allocations[proc_id][1].append((job, duration1))
                        self.allocations[proc_id][0] = w
                    
                    if proc_id + 1 < len(self.processors):
                        self.allocations[proc_id + 1][1].append((job, duration - duration1))
                        self.allocations[proc_id + 1][0] += duration - duration1
                    else:
                        self.sim.logger.log(f"System is not schedulable in HI mode: no slot for {job.name}")

            for allocation in self.allocations:
                if allocation[0] < w:
                    allocation[1].append((None, w - allocation[0]))
            
            for p in range(len(self.processors)):
                print(f"{self.sim.now_ms()}: HI -> Proc {p}", ', '.join(f"Job {x[0].name if x[0] else 'None'}, r: {x[1]}" for x in self.allocations[p][1]))
            return

        for task in hi_tasks:
            job = task.job
            if not job.is_active():
                continue

            duration_hi = ceil(w * task.wcet_hi / job.period)
            duration_lo = ceil(w * task.wcet / job.period)

            if self.allocations[proc_id][0] + duration_hi <= w:
                self.allocations[proc_id][1].append((job, duration_lo))
                # Stub for worst-case
                self.allocations[proc_id][1].append((None, duration_hi - duration_lo))
                self.allocations[proc_id][0] += duration_hi
            else:
                duration1 = w - self.allocations[proc_id][0]

                if duration1 > 0:
                    if duration1 > duration_lo:
                        self.allocations[proc_id][1].append((job, duration_lo))
                        # Stub for worst-case
                        self.allocations[proc_id][1].append((None, duration1 - duration_lo))
                        self.allocations[proc_id][0] = w
                    else:
                        self.allocations[proc_id][1].append((job, duration1))
                        self.allocations[proc_id][0] = w
                    if proc_id + 1 < len(self.processors):
                        if duration1 > duration_lo:
                            self.allocations[proc_id + 1][1].append((None, (duration_hi - duration_lo) - duration1))
                            self.allocations[proc_id + 1][0] += duration_hi - duration1
                        else:
                            self.allocations[proc_id + 1][1].append((job, duration_lo - duration1))
                            self.allocations[proc_id + 1][1].append((None, duration_hi - duration_lo))
                            self.allocations[proc_id + 1][0] += duration_hi - duration1


        for p in range(len(self.processors)):
            print(f"{self.sim.now_ms()}: Proc {p}", ', '.join(f"Job {x[0].name if x[0] else 'None'}, r: {x[1]}" for x in self.allocations[p][1]))
        
        # Remove holes in scheduling -- otherwise processor is not happy
        if self.schedule_type == ScheduleType.HIGH_PRIORITY:
            for proc_id in range(len(self.processors)):
                new_load = 0
                new_allocation = []
                for alloc in self.allocations[proc_id][1]:
                    if alloc[0] is not None:
                        new_allocation.append(alloc)
                        new_load += alloc[1]
                    
                self.allocations[proc_id] = [new_load, new_allocation]

            for allocation in self.allocations:
                if allocation[0] < w:
                    allocation[1].append((None, w - allocation[0]))
            return
        
        # Replace stub by real jobs:
        # Nice O(m * n^2), isn't it?
        proc_id = 0
        lo_jobs_left = {}
        for task in lo_tasks:
            job = task.job
            if not job.is_active():
                continue

            duration = ceil(w * self._get_compute_time_ms(job) / job.period)
            lo_jobs_left[job] = duration

        # The idea is to replace all stubs by real work
        for job in lo_jobs_left.keys():
            for proc_id in range(len(self.processors)):
                if lo_jobs_left[job] == 0:
                    continue

                new_allocation = []
                for alloc in self.allocations[proc_id][1]:
                    if alloc[0] is not None:
                        new_allocation.append(alloc)
                        continue
                    if alloc[1] >= lo_jobs_left[job]:
                        left = lo_jobs_left[job]
                        new_allocation.append((job, left))
                        new_allocation.append((None, alloc[1] - left))
                        lo_jobs_left[job] = 0
                    else:
                        new_allocation.append((job, alloc[1]))
                        lo_jobs_left[job] -= alloc[1]
                self.allocations[proc_id][1] = new_allocation
                
        # At this moment everything is scheduled in stubs slots, or the last proc is not full
        print(f"{self.sim.now_ms()}: Left: {lo_jobs_left.values()}")
        for p in range(len(self.processors)):
            print(f"Proc {p}", ', '.join(f"Job {x[0].name if x[0] else 'None'}, r: {x[1]}" for x in self.allocations[p][1]))
        # Remove stubs (if any)

        for p in range(len(self.processors)):
            new_allocation = []
            new_load = 0
            for alloc in self.allocations[p][1]:
                if alloc[0] is not None:
                    new_allocation.append(alloc)
                    new_load += alloc[1]
            self.allocations[p] = [new_load, new_allocation]
        
        # XXX: there should be something like list.first(cond), right?
        proc_id = self.allocations.index(sorted(self.allocations, key=lambda x: x[0] < w, reverse=True)[0])
        print(f"{self.sim.now_ms()}: Free proc: {proc_id}, l = {self.allocations[proc_id][0] / w}")

        for job in lo_jobs_left.keys():
            if lo_jobs_left[job] == 0:
                continue
            print(f"{self.sim.now_ms()}: Ordering id {job.name}, ret {task.job.ret}, active: {task.job.actual_computation_time}")

            # The "fair" duration for this job on that interval. Rounded to the
            # upper integer to avoid durations that are not multiples of
            # cycles.
            duration = lo_jobs_left[job]
            if self.allocations[proc_id][0] + duration <= w:
                self.allocations[proc_id][1].append((job, duration))
                self.allocations[proc_id][0] += duration
            else:
                # Add first part to the end of the current processor p:
                duration1 = w - self.allocations[proc_id][0]
                if duration1 > 0:
                    self.allocations[proc_id][1].append((job, duration1))
                    self.allocations[proc_id][0] = w

                if proc_id + 1 < len(self.processors):
                    # Add the second part:
                    duration2 = duration - duration1
                    self.allocations[proc_id + 1][1].append((job, duration2))
                    self.allocations[proc_id + 1][0] += duration2
                else:
                    # Because every durations are rounded to the upper value,
                    # the last job may have not enough space left.
                    # This could probably be improved.
                    print("Warning: didn't allowed enough time to last task.",
                          duration - duration1)
                    break

                proc_id += 1

        # Fill with none jobs to make processor happy
        for allocation in self.allocations:
            if allocation[0] < w:
                allocation[1].append((None, w - allocation[0]))

        if self.mirroring:
            for allocation in self.allocations:
                # Rerverse the order of the jobs.
                # Note: swapping the first and last items should be enough.
                allocation[1].reverse()
        self.mirroring = not self.mirroring

        for p in range(len(self.processors)):
            print(f"{self.sim.now_ms()}: Final -> Proc {p}", ', '.join(f"Job {x[0].name if x and x[0] else 'None'}, r: {x[1]}" for x in self.allocations[p][1]))

    def end_event(self, z, job):
        """
        Called when a job's budget has expired.
        """

        self.sim.logger.log(f"End job with id {job.task.identifier}, its ret is {job.ret}, its advance is {job.actual_computation_time}")
        if job.task.is_high_priority and job.actual_computation_time >= job.task.wcet and job.ret > 0.1:
            self._update_type(ScheduleType.HIGH_PRIORITY)
            self.reschedule()
            return
        
        self._update_type(self.schedule_type)
        del self.timers[job]

        l = self.allocations[z][1]
        if l and l[0][0] is job:
            l.pop(0)

        self.reschedule(self.processors[z])

    def schedule(self, cpu):
        """
        Schedule main method.
        """
        self.waiting_schedule = False
        # At the end of the interval:
        if self.sim.now() >= self.t_f or self.previous_schedule_type != self.schedule_type:
            self.sim.logger.log("Interval reinit")
            self.init_interval()

            # Stop current timers.
            for job, timer in self.timers.items():
                timer.stop()
            self.timers = {}

        # Set timers to stop the jobs that will run.
        for z, proc in enumerate(self.processors):
            l = self.allocations[z][1]
            if l and l[0][0] not in self.timers and l[0][0] is not None:
                timer = Timer(self.sim, DP_WRAP_MC.end_event,
                              (self, z, l[0][0]), l[0][1],
                              cpu=proc, in_ms=False)
                timer.start()
                self.sim.logger.log(f"Timer for {l[0][0].task.identifier} is {l[0][1] / self.sim.cycles_per_ms } ahead")
                self.timers[l[0][0]] = timer

        if self.schedule_type == ScheduleType.HIGH_PRIORITY:
            for task in self.task_list:
                if task.job is not None and not task.is_high_priority:
                    task.job.abort()

        # Schedule the activated tasks on each processor.

        for p in range(len(self.processors)):
            print(f"{self.sim.now_ms()}: Decisions -> Proc {p}", ', '.join(f"Job {x[0].name if x[0] else 'None'}, r: {x[1]}" for x in self.allocations[p][1]))
        decisions = []
        for z, proc in enumerate(self.processors):
            l = self.allocations[z][1]
            if not l:
                decisions.append((None, proc))
                # On the next slice
                self._update_type(ScheduleType.LOW_PRORITY)
            
            if not l[0] or not l[0][0] or l[0][0].is_active():
                job = l[0][0]
                if job is None:
                    # On the next slice
                    self._update_type(ScheduleType.LOW_PRORITY)
                    decisions.append((None, proc))
                    continue

                if job.task.is_high_priority:
                    if job.actual_computation_time >= job.task.wcet:
                        self.sim.logger.log("Hello there: scheduling more than expected")
                decisions.append((job, proc))

        dec_str = [f"Job: {x[0].name if x[0] else 'None'} On cpu {x[1].internal_id}" for x in decisions]
        print(f"Returned decision: {', '.join(dec_str)}")
        return decisions
