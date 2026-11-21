from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from heapq import heapify, heappop, heappush


PRIORITY_RANK = {"加急": 0, "普通": 1, "批量": 2}


@dataclass(frozen=True)
class Ticket:
    ticket_id: str
    tenant: str
    priority: str
    arrival_ms: int
    hold_ms: int
    wait_budget_ms: int


@dataclass(frozen=True)
class Timeout:
    pool_used: int
    tenant_used: int


@dataclass(frozen=True)
class Decision:
    ticket: Ticket
    time_ms: int
    timeout: Timeout | None = None

    @property
    def wait_ms(self):
        return self.time_ms - self.ticket.arrival_ms


@dataclass(frozen=True)
class Event:
    time_ms: int
    arrivals: int
    releases: int
    checkouts: int
    timeouts: int
    in_use: int
    waiting: int


class WaitingQueue:
    def __init__(self, tenants):
        self.pending = {}
        self._tenant_queues = {tenant: [] for tenant in tenants}
        self._deadlines = []

    def add(self, ticket):
        self.pending[ticket.ticket_id] = ticket
        heappush(self._tenant_queues[ticket.tenant], (
            PRIORITY_RANK[ticket.priority], ticket.arrival_ms, ticket.ticket_id,
        ))
        heappush(self._deadlines, (ticket.arrival_ms + ticket.wait_budget_ms, ticket.ticket_id))

    def head(self, tenant):
        queue = self._tenant_queues[tenant]
        while queue and queue[0][2] not in self.pending:
            heappop(queue)
        return (*queue[0], tenant) if queue else None

    def claim(self, tenant):
        _, _, ticket_id = heappop(self._tenant_queues[tenant])
        return self.pending.pop(ticket_id)

    def next_deadline(self):
        while self._deadlines and self._deadlines[0][1] not in self.pending:
            heappop(self._deadlines)
        return self._deadlines[0][0] if self._deadlines else None

    def expire(self, time_ms):
        while self.next_deadline() == time_ms:
            _, ticket_id = heappop(self._deadlines)
            yield self.pending.pop(ticket_id)


def replay(pool_size, quotas, tickets):
    arrivals = deque(sorted(tickets, key=lambda ticket: (ticket.arrival_ms, ticket.ticket_id)))
    waiting = WaitingQueue(quotas)
    releases = []
    in_use = dict.fromkeys(quotas, 0)
    peak_by_tenant = in_use.copy()
    decisions = []
    timeline = []

    while True:
        upcoming = (
            arrivals[0].arrival_ms if arrivals else None,
            releases[0][0] if releases else None,
            waiting.next_deadline(),
        )
        now = min((time for time in upcoming if time is not None), default=None)
        if now is None:
            return decisions, timeline, peak_by_tenant

        released = arrived = checked_out = expired = 0
        while releases and releases[0][0] == now:
            _, _, tenant = heappop(releases)
            in_use[tenant] -= 1
            released += 1
        while arrivals and arrivals[0].arrival_ms == now:
            waiting.add(arrivals.popleft())
            arrived += 1

        if waiting.pending and len(releases) < pool_size:
            ready = []
            for tenant, quota in quotas.items():
                if in_use[tenant] < quota and (head := waiting.head(tenant)) is not None:
                    ready.append(head)
            heapify(ready)
            while ready and len(releases) < pool_size:
                _, _, _, tenant = heappop(ready)
                ticket = waiting.claim(tenant)
                heappush(releases, (now + ticket.hold_ms, ticket.ticket_id, tenant))
                decisions.append(Decision(ticket, now))
                in_use[tenant] += 1
                peak_by_tenant[tenant] = max(peak_by_tenant[tenant], in_use[tenant])
                checked_out += 1
                if in_use[tenant] < quotas[tenant] and (head := waiting.head(tenant)) is not None:
                    heappush(ready, head)

        # A ticket at its deadline gets one dispatch opportunity at this timestamp.
        for ticket in waiting.expire(now):
            decisions.append(Decision(ticket, now, Timeout(len(releases), in_use[ticket.tenant])))
            expired += 1

        timeline.append(Event(now, arrived, released, checked_out, expired, len(releases), len(waiting.pending)))
