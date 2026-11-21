from collections import defaultdict

from src.services.pool_replay import PRIORITY_RANK, Ticket, replay


MAX_CONNECTIONS = 512
MAX_OFFSET_MS = 86_400_000
MAX_TICKETS = 5_000
MAX_TENANTS = 100

SPEC = {
    "name": "纸亭便民·印务库会话排队",
    "summary": "回放窗口取号，核对各网点等连接的时间，给值班组分派逾时工单。",
    "customer": "纸亭便民印务 · 门店运营与数据库值班组",
    "entity_labels": ["窗口负载档案", "会话池回放", "逾时与容量凭证"],
    "field_labels": {
        "pool_size": "连接池容量（条）",
        "tenant_quotas": "网点会话配额",
        "tickets": "取号与会话样本",
        "tenant": "网点",
        "quota": "并发会话上限",
        "ticket_id": "取号单",
        "priority": "优先级",
        "arrival_ms": "到达偏移（毫秒）",
        "hold_ms": "连接持有（毫秒）",
        "wait_budget_ms": "等待预算（毫秒）",
    },
    "guidance": "时间统一为相对于同一采样起点的毫秒。优先级依次为加急、普通、批量，预算归零的时刻仍可借用。",
    "sample": {
        "pool_size": 3,
        "tenant_quotas": [
            {"tenant": "东城窗口", "quota": 1},
            {"tenant": "西站窗口", "quota": 2},
            {"tenant": "自助打印", "quota": 2},
        ],
        "tickets": [
            {"ticket_id": "PT-001", "tenant": "东城窗口", "priority": "普通",
             "arrival_ms": 0, "hold_ms": 80, "wait_budget_ms": 20},
            {"ticket_id": "PT-002", "tenant": "西站窗口", "priority": "普通",
             "arrival_ms": 0, "hold_ms": 50, "wait_budget_ms": 20},
            {"ticket_id": "PT-003", "tenant": "东城窗口", "priority": "加急",
             "arrival_ms": 5, "hold_ms": 20, "wait_budget_ms": 25},
            {"ticket_id": "PT-004", "tenant": "自助打印", "priority": "批量",
             "arrival_ms": 10, "hold_ms": 10, "wait_budget_ms": 0},
            {"ticket_id": "PT-005", "tenant": "自助打印", "priority": "普通",
             "arrival_ms": 35, "hold_ms": 75, "wait_budget_ms": 0},
            {"ticket_id": "PT-006", "tenant": "自助打印", "priority": "普通",
             "arrival_ms": 36, "hold_ms": 20, "wait_budget_ms": 7},
            {"ticket_id": "PT-007", "tenant": "西站窗口", "priority": "批量",
             "arrival_ms": 37, "hold_ms": 25, "wait_budget_ms": 70},
            {"ticket_id": "PT-008", "tenant": "西站窗口", "priority": "加急",
             "arrival_ms": 40, "hold_ms": 35, "wait_budget_ms": 10},
            {"ticket_id": "PT-009", "tenant": "东城窗口", "priority": "加急",
             "arrival_ms": 42, "hold_ms": 15, "wait_budget_ms": 18},
            {"ticket_id": "PT-010", "tenant": "东城窗口", "priority": "普通",
             "arrival_ms": 45, "hold_ms": 15, "wait_budget_ms": 65},
        ],
    },
    "expect": {
        "成功借用": 7,
        "等待逾时": 3,
        "成功等待 P95（毫秒）": 48,
        "峰值使用（条）": 3,
        "满池时长（毫秒）": 70,
        "累计占用（连接·毫秒）": 290,
    },
}


def _int_field(record, key, label, minimum, maximum):
    value = record.get(key)
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{label}须为 {minimum} 到 {maximum} 之间的整数")
    return value


def _name_field(record, key, label):
    value = record.get(key)
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > 64:
        raise ValueError(f"{label}须为 1 到 64 字的非空文本")
    return value.strip()


def _rows(payload, key, label, minimum, maximum):
    rows = payload.get(key)
    if not isinstance(rows, list) or not minimum <= len(rows) <= maximum:
        raise ValueError(f"{label}须包含 {minimum} 到 {maximum} 条记录")
    for index, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            raise ValueError(f"{label}第 {index} 条须为对象")
        yield row, f"{label}第 {index} 条"


def _parse(payload):
    if not isinstance(payload, dict):
        raise ValueError("业务输入须为对象")
    pool_size = _int_field(payload, "pool_size", "连接池容量", 1, MAX_CONNECTIONS)
    quotas = {}
    for row, label in _rows(payload, "tenant_quotas", "网点配额", 1, MAX_TENANTS):
        tenant = _name_field(row, "tenant", f"{label}网点")
        if tenant in quotas:
            raise ValueError(f"网点配额重复：{tenant}")
        quotas[tenant] = _int_field(row, "quota", f"{label}并发上限", 1, MAX_CONNECTIONS)

    tickets = []
    seen = set()
    for row, label in _rows(payload, "tickets", "取号样本", 0, MAX_TICKETS):
        ticket_id = _name_field(row, "ticket_id", f"{label}取号单")
        if ticket_id in seen:
            raise ValueError(f"取号单重复：{ticket_id}")
        seen.add(ticket_id)
        tenant = _name_field(row, "tenant", f"{label}网点")
        if tenant not in quotas:
            raise ValueError(f"取号单 {ticket_id} 的网点未设置配额：{tenant}")
        priority = row.get("priority")
        if not isinstance(priority, str) or priority not in PRIORITY_RANK:
            raise ValueError(f"{label}优先级须为加急、普通或批量")
        tickets.append(Ticket(
            ticket_id, tenant, priority,
            _int_field(row, "arrival_ms", f"{label}到达偏移", 0, MAX_OFFSET_MS),
            _int_field(row, "hold_ms", f"{label}连接持有", 1, MAX_OFFSET_MS),
            _int_field(row, "wait_budget_ms", f"{label}等待预算", 0, MAX_OFFSET_MS),
        ))
    return pool_size, quotas, tickets


def validate(payload):
    _parse(payload)


def _ticket_rows(decisions):
    return [{
        "取号单": decision.ticket.ticket_id,
        "网点": decision.ticket.tenant,
        "优先级": decision.ticket.priority,
        "到达（毫秒）": decision.ticket.arrival_ms,
        "等待预算（毫秒）": decision.ticket.wait_budget_ms,
        "实测等待（毫秒）": decision.wait_ms,
        "借用（毫秒）": None if decision.timeout else decision.time_ms,
        "归还（毫秒）": None if decision.timeout else decision.time_ms + decision.ticket.hold_ms,
        "结果": "等待逾时" if decision.timeout else "已完成",
    } for decision in decisions]


def _tenant_rows(quotas, peak_by_tenant, decisions):
    by_tenant = defaultdict(list)
    for decision in decisions:
        by_tenant[decision.ticket.tenant].append(decision)
    rows = []
    for tenant, quota in sorted(quotas.items()):
        tenant_decisions = by_tenant[tenant]
        served = [decision for decision in tenant_decisions if decision.timeout is None]
        rows.append({
            "网点": tenant,
            "配额（条）": quota,
            "峰值使用（条）": peak_by_tenant[tenant],
            "成功借用": len(served),
            "等待逾时": len(tenant_decisions) - len(served),
            "最长等待（毫秒）": max((decision.wait_ms for decision in tenant_decisions), default=None),
            "累计占用（连接·毫秒）": sum(decision.ticket.hold_ms for decision in served),
        })
    return rows


TIMEOUT_ACTIONS = {
    (False, True): ("网点配额已满", "网点调度", "错开同网点批次并排查长会话，再试算调高该网点配额。"),
    (True, False): ("共享池已满", "数据库值班", "定位满池时段的长会话，先错开批次，再按数据库连接预算试算池容量。"),
    (True, True): ("共享池与网点配额均满", "网点调度、数据库值班", "先错开窗口批次并复跑，再联合验证共享池和网点配额。"),
}


def _timeout_rows(timeouts, pool_size, quotas):
    rows = []
    for decision in sorted(timeouts, key=lambda item: (item.time_ms, item.ticket.ticket_id)):
        ticket = decision.ticket
        quota = quotas[ticket.tenant]
        timeout = decision.timeout
        cause, owner, action = TIMEOUT_ACTIONS[timeout.pool_used == pool_size, timeout.tenant_used == quota]
        rows.append({
            "取号单": ticket.ticket_id,
            "网点": ticket.tenant,
            "逾时时刻（毫秒）": decision.time_ms,
            "实测等待（毫秒）": decision.wait_ms,
            "共享池占用": f"{timeout.pool_used}/{pool_size}",
            "网点占用": f"{timeout.tenant_used}/{quota}",
            "到期阻塞": cause,
            "处理方": owner,
            "下一步": action,
        })
    return rows


def solve(payload, engine):
    pool_size, quotas, tickets = _parse(payload)
    decisions, timeline, peak_by_tenant = replay(pool_size, quotas, tickets)
    decisions.sort(key=lambda item: (item.ticket.arrival_ms, item.ticket.ticket_id))
    served = [decision for decision in decisions if decision.timeout is None]
    timeouts = [decision for decision in decisions if decision.timeout is not None]
    waits = sorted(decision.wait_ms for decision in served)
    p95_rank = (95 * len(waits) + 99) // 100
    observed_ms = timeline[-1].time_ms - timeline[0].time_ms if timeline else 0
    capacity_ms = observed_ms * pool_size
    busy_ms = sum(decision.ticket.hold_ms for decision in served)
    full_ms = sum(
        right.time_ms - left.time_ms
        for left, right in zip(timeline, timeline[1:]) if left.in_use == pool_size
    )
    metrics = {
        "成功借用": len(served),
        "等待逾时": len(timeouts),
        "成功等待 P95（毫秒）": waits[p95_rank - 1] if waits else None,
        "最长等待（毫秒）": max((decision.wait_ms for decision in decisions), default=None),
        "峰值排队（单）": max((event.waiting for event in timeline), default=0),
        "池容量（条）": pool_size,
        "峰值使用（条）": max((event.in_use for event in timeline), default=0),
        "满池时长（毫秒）": full_ms,
        "累计占用（连接·毫秒）": busy_ms,
        "观察时长（毫秒）": observed_ms,
        "容量利用率（%）": round(100 * busy_ms / capacity_ms, 2) if capacity_ms else None,
    }
    if timeouts:
        summary = f"{len(decisions)} 单取号中有 {len(timeouts)} 单逾时。请按逾时清单分派处理。"
    elif served:
        summary = f"{len(served)} 单取号均在预算内借到连接。最长等待 {waits[-1]} 毫秒。"
    else:
        summary = "本次样本无取号记录，暂无等待与利用率观测。"
    return {
        "summary": summary,
        "metrics": metrics,
        "sections": [
            {"title": "逾时处理清单", "rows": _timeout_rows(timeouts, pool_size, quotas)},
            {"title": "网点容量", "rows": _tenant_rows(quotas, peak_by_tenant, decisions)},
            {"title": "取号回放", "rows": _ticket_rows(decisions)},
            {"title": "会话占用时间线", "rows": [{
                "时刻（毫秒）": event.time_ms, "到达": event.arrivals, "归还": event.releases,
                "借用": event.checkouts, "逾时": event.timeouts, "占用": event.in_use,
                "空闲": pool_size - event.in_use, "留队": event.waiting,
            } for event in timeline]},
        ],
    }
