#!/usr/bin/env python
import rospy
import time
from collections import deque
import heapq
import threading
from cpu_scheduler.msg import ProcessInfo
from cpu_scheduler.srv import SetPriority, SetPriorityResponse
from std_msgs.msg import String

class Scheduler:
    def __init__(self):
        rospy.init_node('scheduler_node', anonymous=True)
        self.scheduling_algorithm = rospy.get_param('~algorithm', 'FCFS')
        self.time_quantum = rospy.get_param('~time_quantum', 0.1)
        self.preemptive_priority = rospy.get_param('~preemptive_priority', True)
        self.process_queue = deque()
        self.priority_queue = []
        self.process_states = {}
        self.current_running_process_id = None
        self.cpu_grant_start_time = None
        self.scheduler_lock = threading.Lock()
        self.cpu_publisher = rospy.Publisher('/cpu_grant', ProcessInfo, queue_size=10)
        self.event_logger = rospy.Publisher('/scheduling_events', String, queue_size=10)
        rospy.Subscriber('/process_request', ProcessInfo, self.process_request_callback)
        rospy.Subscriber('/process_completion', ProcessInfo, self.process_completion_callback)
        rospy.Service('/set_priority', SetPriority, self.handle_set_priority)
        rospy.loginfo(f"Scheduler Node Started with algorithm: {self.scheduling_algorithm}")
        self.log_event("SCHEDULER_STARTED", f"Algorithm: {self.scheduling_algorithm}, Quantum: {self.time_quantum}")

    def log_event(self, event_type, details=""):
        log_msg = f"{rospy.Time.now().to_sec()}: {event_type} - {details}"
        rospy.loginfo(log_msg)
        self.event_logger.publish(log_msg)

    def process_request_callback(self, msg):
        with self.scheduler_lock:
            process_id = msg.process_id
            burst_time = msg.burst_time
            priority = msg.priority
            arrival_time = rospy.Time.now().to_sec()
            if process_id not in self.process_states:
                self.process_states[process_id] = {
                    'remaining_burst': float(burst_time),
                    'priority': priority,
                    'arrival_time': arrival_time,
                    'start_time': -1,
                    'completion_time': -1,
                    'is_running': False
                }
                if self.scheduling_algorithm == 'FCFS' or self.scheduling_algorithm == 'RR':
                    self.process_queue.append(process_id)
                elif self.scheduling_algorithm == 'Priority':
                    heapq.heappush(self.priority_queue, (priority, arrival_time, process_id))
                self.log_event("PROCESS_REQUESTED", f"ID: {process_id}, Burst: {burst_time}, Priority: {priority}, Arrival: {arrival_time}")
                rospy.loginfo(f"Process {process_id} requested CPU. Current Queue (approx): {self._get_queue_representation()}")
                if self.scheduling_algorithm == 'Priority' and self.preemptive_priority and self.current_running_process_id is not None:
                    current_prio = self.process_states[self.current_running_process_id]['priority']
                    if priority < current_prio:
                        self.log_event("PREEMPTION_TRIGGERED", f"New P{process_id} (Prio:{priority}) vs Current P{self.current_running_process_id} (Prio:{current_prio})")
                        self._preempt_current_process()

    def process_completion_callback(self, msg):
        with self.scheduler_lock:
            completed_process_id = msg.process_id
            if completed_process_id in self.process_states:
                self.process_states[completed_process_id]['completion_time'] = rospy.Time.now().to_sec()
                self.process_states[completed_process_id]['is_running'] = False
                self.log_event("PROCESS_COMPLETED", f"ID: {completed_process_id}")
                rospy.loginfo(f"Process {completed_process_id} completed execution.")
                if self.current_running_process_id == completed_process_id:
                    self.current_running_process_id = None
                    self.cpu_grant_start_time = None
            else:
                rospy.logwarn(f"Received completion for unknown process ID: {completed_process_id}")

    def handle_set_priority(self, req):
        with self.scheduler_lock:
            process_id = req.process_id
            new_priority = req.new_priority
            if process_id in self.process_states:
                old_priority = self.process_states[process_id]['priority']
                self.process_states[process_id]['priority'] = new_priority
                self.log_event("PRIORITY_UPDATED", f"ID: {process_id}, Old Prio: {old_priority}, New Prio: {new_priority}")
                rospy.loginfo(f"Process {process_id} priority updated from {old_priority} to {new_priority}")
                if self.scheduling_algorithm == 'Priority':
                    temp_queue = []
                    for p_prio, p_arrival, p_id in self.priority_queue:
                        if p_id != process_id:
                            heapq.heappush(temp_queue, (p_prio, p_arrival, p_id))
                    self.priority_queue = temp_queue
                    heapq.heapify(self.priority_queue)
                    heapq.heappush(self.priority_queue, (new_priority, self.process_states[process_id]['arrival_time'], process_id))
                    rospy.loginfo(f"Priority queue updated for P{process_id}.")
                    if self.preemptive_priority and self.current_running_process_id is not None:
                        current_prio = self.process_states[self.current_running_process_id]['priority']
                        if new_priority < current_prio:
                            self.log_event("PREEMPTION_TRIGGERED_PRIO_UPDATE", f"P{process_id} new prio {new_priority} vs Current P{self.current_running_process_id} prio {current_prio}")
                            self._preempt_current_process()
                return SetPriorityResponse(True, f"Priority for P{process_id} updated to {new_priority}")
            else:
                rospy.logwarn(f"Process {process_id} not found for priority update.")
                return SetPriorityResponse(False, f"Process {process_id} not found.")

    def _preempt_current_process(self):
        if self.current_running_process_id is not None:
            preempted_id = self.current_running_process_id
            if self.process_states[preempted_id]['is_running']:
                self.log_event("PROCESS_PREEMPTED", f"ID: {preempted_id}")
                rospy.loginfo(f"Scheduler: Preempting P{preempted_id}")
                self.process_states[preempted_id]['is_running'] = False
                self.current_running_process_id = None
                self.cpu_grant_start_time = None
                if self.scheduling_algorithm == 'FCFS' or self.scheduling_algorithm == 'RR':
                    if preempted_id not in self.process_queue:
                        self.process_queue.append(preempted_id)
                elif self.scheduling_algorithm == 'Priority':
                    p_info = self.process_states[preempted_id]
                    heapq.heappush(self.priority_queue, (p_info['priority'], p_info['arrival_time'], preempted_id))

    def _get_next_process_to_run(self):
        if self.scheduling_algorithm == 'FCFS' or self.scheduling_algorithm == 'RR':
            while self.process_queue:
                next_id = self.process_queue.popleft()
                if next_id in self.process_states and self.process_states[next_id]['remaining_burst'] > 0:
                    return next_id
            return None
        elif self.scheduling_algorithm == 'Priority':
            while self.priority_queue:
                prio, arrival, next_id = heapq.heappop(self.priority_queue)
                if next_id in self.process_states and self.process_states[next_id]['remaining_burst'] > 0:
                    if prio == self.process_states[next_id]['priority']:
                        return next_id
                    else:
                        heapq.heappush(self.priority_queue, (self.process_states[next_id]['priority'], self.process_states[next_id]['arrival_time'], next_id))
            return None
        return None

    def _get_queue_representation(self):
        if self.scheduling_algorithm == 'FCFS' or self.scheduling_algorithm == 'RR':
            return list(self.process_queue)
        elif self.scheduling_algorithm == 'Priority':
            return sorted([(prio, pid) for prio, _, pid in self.priority_queue])
        return []

    def run_scheduler(self):
        rate = rospy.Rate(100)
        while not rospy.is_shutdown():
            with self.scheduler_lock:
                if self.scheduling_algorithm == 'RR' and self.current_running_process_id is not None:
                    if self.cpu_grant_start_time is not None and (rospy.Time.now().to_sec() - self.cpu_grant_start_time) >= self.time_quantum:
                        self.log_event("QUANTUM_EXPIRED", f"P{self.current_running_process_id} quantum expired.")
                        self._preempt_current_process()
                if self.current_running_process_id is None:
                    next_process_id = self._get_next_process_to_run()
                    if next_process_id is not None:
                        self.current_running_process_id = next_process_id
                        self.cpu_grant_start_time = rospy.Time.now().to_sec()
                        if self.process_states[next_process_id]['start_time'] == -1:
                            self.process_states[next_process_id]['start_time'] = self.cpu_grant_start_time
                            self.log_event("PROCESS_FIRST_RUN", f"ID: {next_process_id}, Start: {self.cpu_grant_start_time}")
                        self.process_states[next_process_id]['is_running'] = True
                        rospy.loginfo(f"Granting CPU to Process {next_process_id} (Algorithm: {self.scheduling_algorithm})")
                        granted_info = ProcessInfo(process_id=next_process_id, burst_time=0, priority=0)
                        self.cpu_publisher.publish(granted_info)
                        self.log_event("CPU_GRANTED", f"ID: {next_process_id}")
                else:
                    granted_info = ProcessInfo(process_id=self.current_running_process_id, burst_time=0, priority=0)
                    self.cpu_publisher.publish(granted_info)
            rate.sleep()

if __name__ == '__main__':
    try:
        scheduler = Scheduler()
        scheduler.run_scheduler()
    except rospy.ROSInterruptException:
        pass
